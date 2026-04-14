using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Net.Sockets;
using Newtonsoft.Json.Linq;
using NinjaTrader.Cbi;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;

namespace NinjaTrader.NinjaScript.Strategies
{
    /// <summary>
    /// MLAutoStrategy_P1 v2.1 - Bulenox $150K Edition
    /// Integra: P1-Lite ranges + GEXBot levels + Databento ML signals
    /// Sessioni: NOTTE/MATTINA (MR) | POMERIGGIO (OR)
    /// Risk: Max $4000 daily loss | Auto-flat 21:55 CEST
    /// </summary>
    public class MLAutoStrategy_P1 : Strategy
    {
        // ===== CONFIGURAZIONE TCP BRIDGE =====
        private const string TCP_HOST = "127.0.0.1";
        private const int    TCP_PORT = 5555;
        private TcpClient     client;
        private NetworkStream  stream;
        private Thread         listenerThread;
        private volatile bool  isConnected = false;

        // ===== PERCORSI FILE LIVE =====
        private readonly string p1CsvPath   = @"C:\Users\annal\Desktop\p1-lite\data\ninja\range_levels.csv";
        private readonly string gexJsonPath = @"C:\Users\annal\Desktop\WEBSOCKET DATABASE\nt8_live_enhanced.json";

        // ===== CACHE DATI =====
        private DateTime lastP1Read  = DateTime.MinValue;
        private DateTime lastGexRead = DateTime.MinValue;
        private JObject  p1Cache     = new JObject();
        private JObject  gexCache    = new JObject();

        // ===== RISK MANAGEMENT BULENOX $150K =====
        private const double MAX_DAILY_LOSS_USD    = 4000.0;
        private const int    MAX_CONSECUTIVE_LOSSES = 4;
        private const int    AUTO_FLAT_HOUR_UTC     = 19;   // 21:55 CEST = 19:55 UTC
        private const int    AUTO_FLAT_MIN_UTC      = 55;
        private const double MIN_CONFIDENCE         = 0.62;

        private double   dailyPnL           = 0.0;
        private double   dayStartCumProfit  = 0.0;   // CumProfit snapshot at day start
        private double   lastCumProfit      = 0.0;   // CumProfit after last trade
        private int      consecutiveLosses  = 0;
        private int      totalSignals       = 0;
        private DateTime tradingDay         = DateTime.Today;

        // ===== HEARTBEAT =====
        private const int    HEARTBEAT_TIMEOUT_SEC = 30;
        private DateTime     lastHeartbeat         = DateTime.UtcNow;
        private volatile bool pythonAlive           = true;

        // ===== THREAD SAFETY =====
        private readonly object _cacheLock = new object();

        // ===== PARAMETRI STRATEGIA =====
        private int  positionSize    = 2;   // ES contracts
        private bool enableGexFilter = true;
        private bool enableP1Filter  = true;

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name                  = "MLAutoStrategy_P1";
                Calculate             = Calculate.OnEachTick;
                IsOverlay             = false;
                EntriesPerDirection   = 1;
                EntryHandling         = EntryHandling.UniqueEntries;  // NT8 corretto: UniqueEntries (non .Unique)
                IsFillLimitOnTouch    = false;
                OrderFillResolution   = OrderFillResolution.Standard;
                // NOTA: MaximumOrdersAllowed e SignalType NON esistono in NT8 Strategy - rimossi
            }
            else if (State == State.Configure)
            {
                listenerThread = new Thread(TcpListenerLoop)
                {
                    IsBackground = true,
                    Name         = "ML_Signal_Listener"
                };
                listenerThread.Start();

                Print("MLAutoStrategy_P1 v2.1 - Bulenox $150K");
                Print("P1: " + p1CsvPath);
                Print("GEX: " + gexJsonPath);
                Print("Max Loss: $" + MAX_DAILY_LOSS_USD + " | Auto-Flat: " + AUTO_FLAT_HOUR_UTC + ":" + AUTO_FLAT_MIN_UTC + " UTC");
            }
            else if (State == State.Terminated)
            {
                try
                {
                    if (stream != null)  stream.Close();
                    if (client != null)  client.Close();
                }
                catch { }
                Print("Strategia terminata");
            }
        }

        private void TcpListenerLoop()
        {
            while (true)
            {
                try
                {
                    if (!isConnected || client == null || !client.Connected)
                    {
                        try
                        {
                            client      = new TcpClient(TCP_HOST, TCP_PORT);
                            stream      = client.GetStream();
                            isConnected = true;
                            Print("Bridge connesso: " + TCP_HOST + ":" + TCP_PORT);
                        }
                        catch (Exception ex)
                        {
                            isConnected = false;
                            Print("Bridge non disponibile: " + ex.Message);
                            Thread.Sleep(5000);
                            continue;
                        }
                    }

                    if (stream != null && stream.DataAvailable)
                    {
                        byte[] buffer    = new byte[8192];
                        int    bytesRead = stream.Read(buffer, 0, buffer.Length);
                        if (bytesRead > 0)
                        {
                            string json = Encoding.UTF8.GetString(buffer, 0, bytesRead).Trim();
                            if (!string.IsNullOrEmpty(json))
                                ProcessSignal(json);
                        }
                    }

                    Thread.Sleep(100);
                }
                catch (Exception ex)
                {
                    isConnected = false;
                    Print("Listener error: " + ex.Message);
                    Thread.Sleep(3000);
                }
            }
        }

        private void ProcessSignal(string json)
        {
            try
            {
                JObject signal = JObject.Parse(json);

                // Heartbeat from Python
                string msgType = signal["type"] != null ? signal["type"].ToString() : "";
                if (msgType == "HEARTBEAT")
                {
                    lastHeartbeat = DateTime.UtcNow;
                    if (!pythonAlive)
                    {
                        pythonAlive = true;
                        Print("Python heartbeat RESTORED — trading riabilitato");
                    }
                    return;
                }

                string side       = signal["side"] != null ? signal["side"].ToString().ToUpperInvariant() : "";
                double confidence = signal["confidence"] != null ? signal["confidence"].Value<double>() : 0;
                string signalId   = signal["signal_id"] != null ? signal["signal_id"].ToString() : ("SIG_" + DateTime.Now.ToString("HHmmss"));

                if (string.IsNullOrEmpty(side) || confidence < MIN_CONFIDENCE)
                {
                    Print("Segnale scartato: side=" + side + ", conf=" + confidence.ToString("F2"));
                    return;
                }

                RefreshDataCache();

                if (!CheckSessionFilter(side)) return;
                if (enableGexFilter && !CheckGexFilter(side)) return;
                if (!pythonAlive)
                {
                    Print("Segnale rifiutato: Python heartbeat timeout");
                    return;
                }
                if (!CheckRiskManagement()) return;

                ExecuteTrade(side, confidence, signalId);
            }
            catch (Exception ex)
            {
                Print("ProcessSignal error: " + ex.Message);
            }
        }

        private void RefreshDataCache()
        {
            lock (_cacheLock)
            {
                // P1 LEVELS (cache 1 secondo)
                try
                {
                    if ((DateTime.UtcNow - lastP1Read).TotalSeconds >= 1 && File.Exists(p1CsvPath))
                    {
                        string[] lines = File.ReadAllLines(p1CsvPath);
                        if (lines.Length >= 2)
                        {
                            string[] headers = lines[0].Split(',');
                            string[] values  = lines[1].Split(',');
                            p1Cache = new JObject();

                            for (int i = 0; i < Math.Min(headers.Length, values.Length); i++)
                            {
                                string key = headers[i].Trim();
                                string val = values[i].Trim();
                                double numVal;

                                if (double.TryParse(val, out numVal))
                                    p1Cache[key] = numVal;
                                else
                                    p1Cache[key] = val;
                            }
                            lastP1Read = DateTime.UtcNow;
                        }
                    }
                }
                catch { /* Ignora lock temporaneo */ }

                // GEX LEVELS (cache 5 secondi)
                try
                {
                    if ((DateTime.UtcNow - lastGexRead).TotalSeconds >= 5 && File.Exists(gexJsonPath))
                    {
                        string jsonText = File.ReadAllText(gexJsonPath);
                        gexCache    = JObject.Parse(jsonText);
                        lastGexRead = DateTime.UtcNow;
                    }
                }
                catch { /* Ignora errori lettura */ }
            }
        }

        private bool CheckSessionFilter(string side)
        {
            if (!enableP1Filter) return true;

            string mode  = p1Cache["mode"] != null ? p1Cache["mode"].ToString() : "";
            double price = GetCurrentPrice(side);

            // NOTTE/MATTINA: usa MR (Midnight Range)
            if (mode == "MORNING" || mode == "NIGHT")
            {
                double mr1d = GetDouble(p1Cache["mr1d"], 0);
                double mr1u = GetDouble(p1Cache["mr1u"], 0);

                if (side == "LONG" && mr1d > 0 && price > mr1d + 2.0)
                {
                    Print("LONG filtrato: prezzo " + price.ToString("F2") + " > MR1D " + mr1d.ToString("F2") + "+2");
                    return false;
                }
                if (side == "SHORT" && mr1u > 0 && price < mr1u - 2.0)
                {
                    Print("SHORT filtrato: prezzo " + price.ToString("F2") + " < MR1U " + mr1u.ToString("F2") + "-2");
                    return false;
                }
            }

            // POMERIGGIO: usa OR (Open Range)
            if (mode == "AFTERNOON")
            {
                double or1d = GetDouble(p1Cache["or1d"], 0);
                double or1u = GetDouble(p1Cache["or1u"], 0);

                if (side == "LONG" && or1d > 0 && price > or1d + 2.0)
                {
                    Print("LONG filtrato: prezzo " + price.ToString("F2") + " > OR1D " + or1d.ToString("F2") + "+2");
                    return false;
                }
                if (side == "SHORT" && or1u > 0 && price < or1u - 2.0)
                {
                    Print("SHORT filtrato: prezzo " + price.ToString("F2") + " < OR1U " + or1u.ToString("F2") + "-2");
                    return false;
                }
            }

            return true;
        }

        private bool CheckGexFilter(string side)
        {
            double price     = GetCurrentPrice(side);
            double callWall  = GetDouble(gexCache["call_wall"], 0);
            double putWall   = GetDouble(gexCache["put_wall"], 0);
            double zeroGamma = GetDouble(gexCache["zero_gamma"], 0);
            string regime    = gexCache["gex_regime"] != null ? gexCache["gex_regime"].ToString() : "neutral";

            if (callWall > 0 && side == "LONG" && price > callWall - 3.0)
            {
                Print("LONG bloccato: prezzo vicino/sopra CallWall " + callWall.ToString("F2"));
                return false;
            }
            if (putWall > 0 && side == "SHORT" && price < putWall + 3.0)
            {
                Print("SHORT bloccato: prezzo vicino/sotto PutWall " + putWall.ToString("F2"));
                return false;
            }

            if (regime == "negative")
            {
                double conf = GetDouble(gexCache["ml_confidence"], 0);
                if (conf < 0.75)
                {
                    Print("Negative gamma regime: conf " + conf.ToString("F2") + " < 0.75, segnale scartato");
                    return false;
                }
            }

            if (zeroGamma > 0 && Math.Abs(price - zeroGamma) < 5.0)
            {
                Print("Prezzo vicino ZeroGamma " + zeroGamma.ToString("F2") + ", attesa conferma");
                return false;
            }

            return true;
        }

        private bool CheckRiskManagement()
        {
            DateTime now = DateTime.UtcNow;

            if (now.Date != tradingDay)
            {
                double cumProfit = SystemPerformance.AllTrades.TradesPerformance.Currency.CumProfit;
                dayStartCumProfit = cumProfit;
                lastCumProfit     = cumProfit;
                dailyPnL          = 0;
                consecutiveLosses = 0;
                tradingDay        = now.Date;
                Print("Nuovo giorno: P&L reset a $" + dailyPnL);
            }

            // =============================================================
            // BLOCCO OVERNIGHT TCP: 19:55-22:00 UTC (= 21:55-00:00 CEST)
            // Rifiuta OGNI segnale TCP durante questo periodo.
            // Copre anche CET invernale (20:55-23:00) — più conservativo.
            // REGOLA PROP FIRM: MAI portare contratti overnight.
            // =============================================================
            int utcH = now.Hour;
            int utcM = now.Minute;
            if ((utcH == 19 && utcM >= 55) || utcH == 20 || utcH == 21)
            {
                if (Position.MarketPosition != MarketPosition.Flat)
                {
                    Print("*** TCP NO OVERNIGHT *** Chiusura " + Position.MarketPosition + " UTC=" + utcH + ":" + utcM);
                    if (Position.MarketPosition == MarketPosition.Long)
                        ExitLong("TCP_NO_OVERNIGHT");
                    else
                        ExitShort("TCP_NO_OVERNIGHT");
                }
                return false;  // BLOCCA ogni segnale TCP
            }

            // MAX DAILY LOSS
            if (dailyPnL <= -MAX_DAILY_LOSS_USD)
            {
                Print("MAX DAILY LOSS: $" + dailyPnL.ToString("F2") + " <= -$" + MAX_DAILY_LOSS_USD);
                if (Position.MarketPosition != MarketPosition.Flat)
                {
                    if (Position.MarketPosition == MarketPosition.Long)
                        ExitLong("MAX_DAILY_LOSS");
                    else
                        ExitShort("MAX_DAILY_LOSS");
                }
                return false;
            }

            // MAX CONSECUTIVE LOSSES
            if (consecutiveLosses >= MAX_CONSECUTIVE_LOSSES)
            {
                Print("Max consecutive losses: " + consecutiveLosses);
                return false;
            }

            return true;
        }

        private void ExecuteTrade(string side, double confidence, string signalId)
        {
            double price = GetCurrentPrice(side);
            double sl = 0;
            double tp = 0;

            if (side == "LONG")
            {
                double target = GetDouble(p1Cache["live_r2_up"], GetDouble(p1Cache["mr2d"], price + 20));
                double stop   = GetDouble(p1Cache["live_r1_dn"], GetDouble(p1Cache["mr1d"], price - 10));

                tp = target > price ? target : price + 15;
                sl = stop < price   ? stop   : price - 8;

                EnterLong(positionSize, "ML_L_" + signalId);
                SetStopLoss(CalculationMode.Price, sl);
                SetProfitTarget(CalculationMode.Price, tp);
            }
            else if (side == "SHORT")
            {
                double target = GetDouble(p1Cache["live_r2_dn"], GetDouble(p1Cache["mr2d"], price - 20));
                double stop   = GetDouble(p1Cache["live_r1_up"], GetDouble(p1Cache["mr1u"], price + 10));

                tp = target < price ? target : price - 15;
                sl = stop > price   ? stop   : price + 8;

                EnterShort(positionSize, "ML_S_" + signalId);
                SetStopLoss(CalculationMode.Price, sl);
                SetProfitTarget(CalculationMode.Price, tp);
            }

            totalSignals++;
            Print("EXEC #" + totalSignals + " " + side + " | Entry:" + price.ToString("F2") + " SL:" + sl.ToString("F2") + " TP:" + tp.ToString("F2") + " | Conf:" + confidence.ToString("F2") + " | ID:" + signalId);
        }

        private double GetCurrentPrice(string side)
        {
            return side == "LONG" ? GetCurrentAsk() : GetCurrentBid();
        }

        private double GetDouble(JToken token, double defaultValue)
        {
            if (token == null)
                return defaultValue;
            try { return token.Value<double>(); }
            catch { return defaultValue; }
        }

        protected override void OnExecutionUpdate(Execution execution, string executionId,
            double price, int quantity, MarketPosition marketPosition,
            string orderId, DateTime time)
        {
            base.OnExecutionUpdate(execution, executionId, price, quantity, marketPosition, orderId, time);

            // Tracciamento quando posizione torna flat
            if (marketPosition == MarketPosition.Flat && Position.Quantity == 0)
            {
                double cumProfit = SystemPerformance.AllTrades.TradesPerformance.Currency.CumProfit;

                if (DateTime.UtcNow.Date != tradingDay)
                {
                    dayStartCumProfit = cumProfit;
                    consecutiveLosses = 0;
                    tradingDay        = DateTime.UtcNow.Date;
                }

                double tradePnL = cumProfit - lastCumProfit;
                lastCumProfit   = cumProfit;
                dailyPnL        = cumProfit - dayStartCumProfit;

                if (tradePnL < 0)
                {
                    consecutiveLosses++;
                    Print("Trade in perdita: $" + tradePnL.ToString("F2") + " | Streak: " + consecutiveLosses);
                }
                else
                {
                    consecutiveLosses = 0;
                    Print("Trade in profitto: $" + tradePnL.ToString("F2"));
                }

                Print("Daily P&L: $" + dailyPnL.ToString("F2") + " / -$" + MAX_DAILY_LOSS_USD);
            }
        }

        protected override void OnBarUpdate()
        {
            // ===== BLOCCO WEEKEND ASSOLUTO =====
            if (Time[0].DayOfWeek == DayOfWeek.Saturday || Time[0].DayOfWeek == DayOfWeek.Sunday)
            {
                if (Position.MarketPosition != MarketPosition.Flat)
                {
                    Print("BLOCCO WEEKEND: chiusura forzata " + Position.MarketPosition);
                    if (Position.MarketPosition == MarketPosition.Long)
                        ExitLong("WEEKEND_FLAT");
                    else
                        ExitShort("WEEKEND_FLAT");
                }
                return;
            }

            int currentTime = ToTime(Time[0]); // HHMMSS nel fuso sessione

            // =============================================================
            // REGOLA PROP FIRM ASSOLUTA: MAI PORTARE CONTRATTI OVERNIGHT
            // Chiudi TUTTO alle 21:55 CEST. ZERO trade dalle 21:55 alle 00:00.
            // Se per qualsiasi motivo una posizione è aperta dopo le 21:55,
            // questa viene chiusa IMMEDIATAMENTE ad ogni tick.
            // =============================================================
            if (currentTime >= 215500)
            {
                if (Position.MarketPosition != MarketPosition.Flat)
                {
                    Print("*** NO OVERNIGHT *** Chiusura OBBLIGATORIA " + Position.MarketPosition + " | Time=" + currentTime);
                    if (Position.MarketPosition == MarketPosition.Long)
                        ExitLong("NO_OVERNIGHT");
                    else
                        ExitShort("NO_OVERNIGHT");
                }
                return; // BLOCCO TOTALE: nessun nuovo trade fino a mezzanotte
            }

            // ===== RESET GIORNALIERO PnL =====
            if (Time[0].Date != tradingDay)
            {
                double prevPnL = dailyPnL;
                double cumProfit = SystemPerformance.AllTrades.TradesPerformance.Currency.CumProfit;
                dayStartCumProfit = cumProfit;
                lastCumProfit     = cumProfit;
                dailyPnL          = 0;
                consecutiveLosses = 0;
                tradingDay        = Time[0].Date;
                Print("NUOVO GIORNO: PnL reset (ieri: $" + prevPnL.ToString("F2") + ")");
            }

            // ===== SAFETY: MAX DAILY LOSS CHECK CONTINUO =====
            if (dailyPnL <= -MAX_DAILY_LOSS_USD)
            {
                if (Position.MarketPosition != MarketPosition.Flat)
                {
                    Print("MAX DAILY LOSS OnBarUpdate: $" + dailyPnL.ToString("F2"));
                    if (Position.MarketPosition == MarketPosition.Long)
                        ExitLong("DAILY_LOSS_BAR");
                    else
                        ExitShort("DAILY_LOSS_BAR");
                }
                return;
            }

            // ===== HEARTBEAT CHECK =====
            double secsSinceHeartbeat = (DateTime.UtcNow - lastHeartbeat).TotalSeconds;
            if (secsSinceHeartbeat > HEARTBEAT_TIMEOUT_SEC)
            {
                if (pythonAlive)
                {
                    pythonAlive = false;
                    Print("*** PYTHON HEARTBEAT TIMEOUT *** " + secsSinceHeartbeat.ToString("F0") + "s — trading DISABILITATO");
                }

                // Close open positions if Python is dead
                if (Position.MarketPosition != MarketPosition.Flat)
                {
                    Print("Chiusura forzata per heartbeat timeout");
                    if (Position.MarketPosition == MarketPosition.Long)
                        ExitLong("HB_TIMEOUT");
                    else
                        ExitShort("HB_TIMEOUT");
                }
                return; // Block all new signals
            }

            // Logica segnali gestita via TCP (TcpListenerLoop -> ProcessSignal)
        }
    }
}
