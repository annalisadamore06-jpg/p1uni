using System;
using System.IO;
using System.Windows.Media;
using Newtonsoft.Json.Linq;
using NinjaTrader.Cbi;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.Gui.Tools;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.DrawingTools;

namespace NinjaTrader.NinjaScript.Indicators
{
    /// <summary>
    /// MLSignalIndicator v3.0 — Livelli P1-Lite + GEXBot su grafico NT8
    ///
    /// === LIVELLI P1-LITE (Range Levels) ===
    ///   MR1 Down / MR1 Up      — Midnight Range Level 1 (supporto/resistenza primari notte)
    ///   MR2 Down / MR2 Up      — Midnight Range Level 2 (estensioni notte)
    ///   OR1 Down / OR1 Up      — Open Range Level 1 (supporto/resistenza primari pomeriggio)
    ///   OR2 Down / OR2 Up      — Open Range Level 2 (estensioni pomeriggio)
    ///   Live R1 Down / R1 Up   — Range dinamico livello 1
    ///   Live R2 Down / R2 Up   — Range dinamico livello 2
    ///
    /// === LIVELLI GEXBOT (Options Flow) ===
    ///   Call Wall               — Resistenza massima da options (muro di call)
    ///   Put Wall                — Supporto massimo da options (muro di put)
    ///   Zero Gamma              — Punto di flip gamma (sopra = stabile, sotto = volatile)
    ///
    /// === BACKGROUND ===
    ///   Verde scuro = Gamma Positivo (mercato stabile, mean-reversion)
    ///   Rosso scuro = Gamma Negativo (mercato volatile, trend-following)
    /// </summary>
    public class MLSignalIndicator : Indicator
    {
        // === PERCORSI FILE LIVE ===
        private readonly string p1CsvPath   = @"C:\Users\annal\Desktop\p1-lite\data\ninja\range_levels.csv";
        private readonly string gexJsonPath = @"C:\Users\annal\Desktop\WEBSOCKET DATABASE\nt8_live_enhanced.json";

        // === CACHE ===
        private DateTime lastRefresh = DateTime.MinValue;
        private JObject  data        = new JObject();

        // === COLORI P1 ===
        private static readonly Brush MR_DOWN_COLOR    = Brushes.DeepSkyBlue;   // Livelli MR sotto
        private static readonly Brush MR_UP_COLOR      = Brushes.Magenta;        // Livelli MR sopra
        private static readonly Brush OR_DOWN_COLOR    = Brushes.DodgerBlue;     // Livelli OR sotto
        private static readonly Brush OR_UP_COLOR      = Brushes.MediumOrchid;   // Livelli OR sopra
        private static readonly Brush LIVE_DOWN_COLOR  = Brushes.Aqua;           // Live range sotto
        private static readonly Brush LIVE_UP_COLOR    = Brushes.HotPink;        // Live range sopra

        // === COLORI GEX ===
        private static readonly Brush CALL_WALL_COLOR  = Brushes.Lime;           // Call Wall
        private static readonly Brush PUT_WALL_COLOR   = Brushes.OrangeRed;      // Put Wall
        private static readonly Brush ZERO_GAMMA_COLOR = Brushes.Gold;           // Zero Gamma

        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name              = "MLSignalIndicator";
                Description       = "P1-Lite Ranges + GEXBot Options Levels";
                Calculate         = Calculate.OnEachTick;
                IsOverlay         = true;
                DrawOnPricePanel  = true;
                PaintPriceMarkers = false;

                // Dummy plot per validazione NT8 (non usato visivamente)
                AddPlot(new Stroke(Brushes.Transparent, 0), PlotStyle.Line, "Dummy");
            }
        }

        protected override void OnBarUpdate()
        {
            // Refresh ogni 5 secondi per non sovraccaricare I/O
            if ((DateTime.UtcNow - lastRefresh).TotalSeconds < 5)
                return;

            lastRefresh = DateTime.UtcNow;

            try { RefreshData(); }
            catch { /* Ignora errori file lock temporanei */ }

            // === GEX DATA ===
            double cw     = GetDouble(data["call_wall"]);
            double pw     = GetDouble(data["put_wall"]);
            double zg     = GetDouble(data["zero_gamma"]);
            string regime = data["gex_regime"] != null ? data["gex_regime"].ToString() : "neutral";
            string mode   = data["mode"] != null ? data["mode"].ToString() : "?";

            // ==========================================================
            //  P1-LITE LEVELS — Midnight Range (sessione notturna/mattina)
            // ==========================================================
            DrawLevel("P1_MR1_Down",   GetDouble(data["mr1d"]),       MR_DOWN_COLOR, DashStyleHelper.Dash, 2, "MR1 Down");
            DrawLevel("P1_MR1_Up",     GetDouble(data["mr1u"]),       MR_UP_COLOR,   DashStyleHelper.Dash, 2, "MR1 Up");
            DrawLevel("P1_MR2_Down",   GetDouble(data["mr2d"]),       MR_DOWN_COLOR, DashStyleHelper.Dot,  1, "MR2 Down");
            DrawLevel("P1_MR2_Up",     GetDouble(data["mr2u"]),       MR_UP_COLOR,   DashStyleHelper.Dot,  1, "MR2 Up");

            // ==========================================================
            //  P1-LITE LEVELS — Open Range (sessione pomeridiana)
            // ==========================================================
            DrawLevel("P1_OR1_Down",   GetDouble(data["or1d"]),       OR_DOWN_COLOR, DashStyleHelper.Dash, 2, "OR1 Down");
            DrawLevel("P1_OR1_Up",     GetDouble(data["or1u"]),       OR_UP_COLOR,   DashStyleHelper.Dash, 2, "OR1 Up");
            DrawLevel("P1_OR2_Down",   GetDouble(data["or2d"]),       OR_DOWN_COLOR, DashStyleHelper.Dot,  1, "OR2 Down");
            DrawLevel("P1_OR2_Up",     GetDouble(data["or2u"]),       OR_UP_COLOR,   DashStyleHelper.Dot,  1, "OR2 Up");

            // ==========================================================
            //  P1-LITE LEVELS — Live Dynamic Range
            // ==========================================================
            DrawLevel("P1_LiveR1_Down", GetDouble(data["live_r1_dn"]), LIVE_DOWN_COLOR, DashStyleHelper.Solid, 1, "Live R1 Dn");
            DrawLevel("P1_LiveR1_Up",   GetDouble(data["live_r1_up"]), LIVE_UP_COLOR,   DashStyleHelper.Solid, 1, "Live R1 Up");
            DrawLevel("P1_LiveR2_Down", GetDouble(data["live_r2_dn"]), LIVE_DOWN_COLOR, DashStyleHelper.DashDot, 1, "Live R2 Dn");
            DrawLevel("P1_LiveR2_Up",   GetDouble(data["live_r2_up"]), LIVE_UP_COLOR,   DashStyleHelper.DashDot, 1, "Live R2 Up");

            // ==========================================================
            //  GEXBOT LEVELS — Options Flow
            // ==========================================================
            DrawLevel("GEX_CallWall",  cw, CALL_WALL_COLOR,  DashStyleHelper.Solid, 3, "CALL WALL");
            DrawLevel("GEX_PutWall",   pw, PUT_WALL_COLOR,   DashStyleHelper.Solid, 3, "PUT WALL");
            DrawLevel("GEX_ZeroGamma", zg, ZERO_GAMMA_COLOR, DashStyleHelper.Solid, 3, "ZERO GAMMA");

            // ==========================================================
            //  BACKGROUND COLORE REGIME GAMMA
            // ==========================================================
            if (regime == "positive")
                BackBrush = new SolidColorBrush(Color.FromArgb(40, 0, 180, 0));    // Verde trasparente
            else if (regime == "negative")
                BackBrush = new SolidColorBrush(Color.FromArgb(40, 180, 0, 0));    // Rosso trasparente
            else
                BackBrush = null;

            // ==========================================================
            //  INFO PANEL (angolo alto destra)
            // ==========================================================
            string regimeLabel = regime == "positive" ? "POS" : (regime == "negative" ? "NEG" : "NEU");

            string infoText =
                "=== P1-LITE ===" +
                "\nMode: " + mode +
                "\nMR1: " + FormatPrice(GetDouble(data["mr1d"])) + " / " + FormatPrice(GetDouble(data["mr1u"])) +
                "\nMR2: " + FormatPrice(GetDouble(data["mr2d"])) + " / " + FormatPrice(GetDouble(data["mr2u"])) +
                "\nOR1: " + FormatPrice(GetDouble(data["or1d"])) + " / " + FormatPrice(GetDouble(data["or1u"])) +
                "\nOR2: " + FormatPrice(GetDouble(data["or2d"])) + " / " + FormatPrice(GetDouble(data["or2u"])) +
                "\nLive R1: " + FormatPrice(GetDouble(data["live_r1_dn"])) + " / " + FormatPrice(GetDouble(data["live_r1_up"])) +
                "\nLive R2: " + FormatPrice(GetDouble(data["live_r2_dn"])) + " / " + FormatPrice(GetDouble(data["live_r2_up"])) +
                "\n" +
                "\n=== GEXBOT ===" +
                "\nGamma: " + regimeLabel +
                "\nCall Wall: " + FormatPrice(cw) +
                "\nPut Wall:  " + FormatPrice(pw) +
                "\nZero Gamma: " + FormatPrice(zg);

            Draw.TextFixed(this, "ML_INFO_PANEL", infoText, TextPosition.TopRight,
                Brushes.White, new SimpleFont("Consolas", 10),
                Brushes.Transparent, Brushes.Black, 80);

            // === LABEL SESSIONE (angolo alto sinistra) ===
            string sessionLabel = "SESSION: " + mode + " | GAMMA: " + regimeLabel;
            Brush sessionColor = regime == "positive" ? Brushes.LimeGreen :
                                 regime == "negative" ? Brushes.Tomato : Brushes.White;

            Draw.TextFixed(this, "ML_SESSION", sessionLabel, TextPosition.TopLeft,
                sessionColor, new SimpleFont("Consolas", 12),
                Brushes.Transparent, Brushes.Black, 80);
        }

        // ==========================================================
        //  DISEGNO LIVELLI CON LABEL
        // ==========================================================
        private void DrawLevel(string tag, double level, Brush color, DashStyleHelper style, int width, string label)
        {
            if (level <= 0 || double.IsNaN(level) || double.IsInfinity(level))
            {
                RemoveDrawObject(tag);
                RemoveDrawObject(tag + "_LBL");
                return;
            }

            // Linea orizzontale
            Draw.HorizontalLine(this, tag, true, level, color, style, width);

            // Label sulla linea (bordo destro)
            string priceLabel = label + "  " + level.ToString("F2");
            Draw.Text(this, tag + "_LBL", true, priceLabel,
                -3, level, 0, color,
                new SimpleFont("Consolas", 9), TextAlignment.Right,
                Brushes.Transparent, Brushes.Transparent, 0);
        }

        // ==========================================================
        //  LETTURA DATI
        // ==========================================================
        private void RefreshData()
        {
            // P1-Lite CSV
            if (File.Exists(p1CsvPath))
            {
                string[] lines = File.ReadAllLines(p1CsvPath);
                if (lines.Length >= 2)
                {
                    string[] headers = lines[0].Split(',');
                    string[] values  = lines[1].Split(',');

                    for (int i = 0; i < Math.Min(headers.Length, values.Length); i++)
                    {
                        string key = headers[i].Trim();
                        double val;
                        if (double.TryParse(values[i].Trim(), System.Globalization.NumberStyles.Any,
                            System.Globalization.CultureInfo.InvariantCulture, out val))
                            data[key] = val;
                        else
                            data[key] = values[i].Trim();
                    }
                }
            }

            // GEXBot JSON
            if (File.Exists(gexJsonPath))
            {
                string jsonText = File.ReadAllText(gexJsonPath);
                JObject gex = JObject.Parse(jsonText);
                data.Merge(gex, new JsonMergeSettings { MergeArrayHandling = MergeArrayHandling.Replace });
            }
        }

        private double GetDouble(JToken token, double fallback = 0)
        {
            if (token == null) return fallback;
            try { return token.Value<double>(); }
            catch { return fallback; }
        }

        private string FormatPrice(double price)
        {
            if (price <= 0) return "---";
            return price.ToString("F2");
        }
    }
}
