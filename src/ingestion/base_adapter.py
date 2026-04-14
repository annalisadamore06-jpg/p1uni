"""
BaseAdapter - Classe madre di tutti gli adapter di ingestion.

Gestisce il ciclo di vita comune: ricezione dati, buffer, batch writing,
validazione, quarantena, auto-reconnect, metriche.

ARCHITETTURA:
  Feed esterno -> connect()/listen() -> process_message() -> _queue
       |                                                        |
       |   (thread-safe queue, max 10k, backpressure alert)     |
       |                                                        v
       |                                              _batch_writer_loop()
       |                                              (ogni 5s o 500 record)
       |                                                        |
       |                                              validate_batch_and_quarantine()
       |                                                  /           \
       |                                          valid_df          rejected_df
       |                                             |                  |
       |                                        _write_to_db()   _write_to_quarantine()
       |                                             |
       v                                        DuckDB (singola transazione)
  auto-reconnect (backoff esponenziale)

THREADING:
  - Usa threading (non asyncio) perche' DuckDB non e' async-safe
  - Queue: queue.Queue (thread-safe built-in Python)
  - 2 thread per adapter: listener + batch_writer
  - Metriche: contatori atomici via threading.Lock

Le sottoclassi implementano solo: connect(), listen(), disconnect()
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.core.database import DatabaseManager
from src.core.normalizer import UniversalNormalizer
from src.core.validator import DataValidator

logger = logging.getLogger("p1uni.ingestion.base")


# ============================================================
# Metriche thread-safe
# ============================================================

class AdapterMetrics:
    """Contatori thread-safe per monitoring.

    Esposti a system_monitor.py via get_stats().
    """

    __slots__ = (
        "_lock", "records_received", "records_validated_ok",
        "records_quarantined", "batches_written", "batch_errors",
        "reconnect_count", "last_write_ts",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.records_received: int = 0
        self.records_validated_ok: int = 0
        self.records_quarantined: int = 0
        self.batches_written: int = 0
        self.batch_errors: int = 0
        self.reconnect_count: int = 0
        self.last_write_ts: datetime | None = None

    def inc_received(self, n: int = 1) -> None:
        with self._lock:
            self.records_received += n

    def inc_validated(self, n: int = 1) -> None:
        with self._lock:
            self.records_validated_ok += n

    def inc_quarantined(self, n: int = 1) -> None:
        with self._lock:
            self.records_quarantined += n

    def inc_batches(self) -> None:
        with self._lock:
            self.batches_written += 1
            self.last_write_ts = datetime.now(timezone.utc)

    def inc_batch_errors(self) -> None:
        with self._lock:
            self.batch_errors += 1

    def inc_reconnects(self) -> None:
        with self._lock:
            self.reconnect_count += 1

    def get_stats(self) -> dict[str, Any]:
        """Snapshot delle metriche per system_monitor."""
        with self._lock:
            return {
                "records_received": self.records_received,
                "records_validated_ok": self.records_validated_ok,
                "records_quarantined": self.records_quarantined,
                "batches_written": self.batches_written,
                "batch_errors": self.batch_errors,
                "reconnect_count": self.reconnect_count,
                "last_write_ts": self.last_write_ts.isoformat() if self.last_write_ts else None,
                "queue_size": 0,  # override in BaseAdapter.get_stats()
            }


# ============================================================
# BaseAdapter
# ============================================================

class BaseAdapter(ABC):
    """Classe base astratta per tutti gli adapter di ingestion.

    Le sottoclassi implementano:
    - connect(): stabilisce la connessione alla fonte
    - listen(): loop di ricezione dati (chiama self.process_message per ogni record)
    - disconnect(): chiude la connessione

    BaseAdapter gestisce automaticamente:
    - Buffer thread-safe con backpressure
    - Batch writing ogni 5s o 500 record
    - Validazione e quarantena
    - Auto-reconnect con backoff esponenziale
    - Metriche per monitoring
    """

    # Configurazione batch writer
    BATCH_SIZE: int = 500           # Flush quando la queue raggiunge questo numero
    FLUSH_INTERVAL_SEC: float = 5.0 # Flush ogni N secondi anche se < BATCH_SIZE
    QUEUE_MAX_SIZE: int = 10_000    # Backpressure threshold
    QUEUE_WARN_SIZE: int = 8_000    # Warning Telegram a questo livello

    # Auto-reconnect
    RECONNECT_BASE_SEC: float = 5.0    # start higher to avoid connection flooding
    RECONNECT_MAX_SEC: float = 120.0   # max 2 min between retries

    # Retry scrittura DB
    DB_RETRY_DELAYS: tuple[float, ...] = (1.0, 2.0, 4.0)

    def __init__(
        self,
        adapter_name: str,
        source_type: str,
        target_table: str,
        config: dict[str, Any],
        db: DatabaseManager,
        telegram: Any = None,
    ) -> None:
        """
        Args:
            adapter_name: Nome identificativo (es: "databento", "gexbot_ws", "p1lite")
            source_type: Tag fonte per normalizer ('WS', 'DATABENTO', 'P1_LITE', etc.)
            target_table: Tabella DuckDB destinazione ('trades_live', 'gex_summary', etc.)
            config: Configurazione globale (settings.yaml)
            db: DatabaseManager singleton
            telegram: TelegramBot per alert (opzionale)
        """
        self.adapter_name = adapter_name
        self.source_type = source_type
        self.target_table = target_table
        self.config = config
        self.db = db
        self.telegram = telegram

        # Componenti
        self.normalizer = UniversalNormalizer()
        self.validator = DataValidator(db)
        self.metrics = AdapterMetrics()

        # Queue thread-safe
        self._queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        self._backpressure_warned: bool = False

        # Stato
        self._running: bool = False
        self._shutdown_event = threading.Event()

        # Alert rate limiting: max 1 alert ogni 5 minuti per adapter
        self._last_alert_time: float = 0.0
        self._alert_cooldown_sec: float = 300.0  # 5 minuti

        # Path quarantena file
        base_dir = Path(config.get("_base_dir", "."))
        self._quarantine_dir = base_dir / "data" / "quarantine"

        # Logger specifico per adapter
        self._log = logging.getLogger(f"p1uni.ingestion.{adapter_name}")

    # ============================================================
    # Metodi astratti (le sottoclassi DEVONO implementare)
    # ============================================================

    @abstractmethod
    def connect(self) -> None:
        """Stabilisce la connessione alla fonte dati.

        Raises:
            ConnectionError: se la connessione fallisce.
        """

    @abstractmethod
    def listen(self) -> None:
        """Loop di ricezione dati. Deve chiamare self.process_message()
        per ogni record ricevuto.

        Questo metodo blocca finche' self._running e' True.
        Deve uscire quando self._shutdown_event e' set.
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Chiude la connessione alla fonte dati."""

    # ============================================================
    # Lifecycle: start / stop
    # ============================================================

    def start(self) -> None:
        """Avvia l'adapter: listener thread + batch writer thread.

        Chiamare da main.py. Non blocca (avvia 2 daemon thread).
        """
        self._running = True
        self._shutdown_event.clear()

        # Thread 1: batch writer (consuma la queue, scrive su DB)
        self._writer_thread = threading.Thread(
            target=self._batch_writer_loop,
            name=f"{self.adapter_name}-writer",
            daemon=True,
        )
        self._writer_thread.start()

        # Thread 2: listener con auto-reconnect
        self._listener_thread = threading.Thread(
            target=self._listener_loop_with_reconnect,
            name=f"{self.adapter_name}-listener",
            daemon=True,
        )
        self._listener_thread.start()

        self._log.info(f"Adapter '{self.adapter_name}' started (table={self.target_table})")

    def stop(self) -> None:
        """Shutdown ordinato: ferma listener, flush finale, chiude."""
        self._log.info(f"Stopping adapter '{self.adapter_name}'...")
        self._running = False
        self._shutdown_event.set()

        # Flush finale: scrivi tutto quello che resta nella queue
        self._flush_queue()

        self._log.info(
            f"Adapter '{self.adapter_name}' stopped. "
            f"Stats: {self.metrics.get_stats()}"
        )

    # ============================================================
    # Listener con auto-reconnect
    # ============================================================

    def _listener_loop_with_reconnect(self) -> None:
        """Loop esterno: connect + listen con auto-reconnect su errore."""
        retry_delay = self.RECONNECT_BASE_SEC

        while self._running and not self._shutdown_event.is_set():
            try:
                self._log.info(f"Connecting to {self.adapter_name}...")
                self.connect()
                retry_delay = self.RECONNECT_BASE_SEC  # reset on success
                self._log.info(f"Connected. Listening...")
                self.listen()

            except Exception as e:
                if not self._running:
                    break
                self.metrics.inc_reconnects()
                self._log.error(
                    f"Connection lost: {e}. Reconnecting in {retry_delay:.0f}s..."
                )
                self._send_alert(
                    f"Adapter {self.adapter_name} disconnected: {e}. "
                    f"Retry in {retry_delay:.0f}s",
                    level="WARNING",
                )

                # Backoff esponenziale
                self._shutdown_event.wait(retry_delay)
                retry_delay = min(retry_delay * 2, self.RECONNECT_MAX_SEC)

            finally:
                try:
                    self.disconnect()
                except Exception:
                    pass

    # ============================================================
    # process_message: entry point per i dati in arrivo
    # ============================================================

    def process_message(self, raw_data: dict[str, Any]) -> None:
        """Processa un singolo record grezzo dalla fonte.

        1. Normalizza via UniversalNormalizer
        2. Accoda nel buffer thread-safe
        3. Gestisce backpressure

        Chiamato dalla sottoclasse dentro listen().
        NON fa validazione qui (la fa il batch writer).
        """
        try:
            # Normalizza (immutabile, crea copia)
            normalized = self.normalizer.normalize(raw_data, self.source_type)
            self.metrics.inc_received()

            # Accoda
            try:
                self._queue.put_nowait(normalized)
            except queue.Full:
                # Backpressure: queue piena, scarta il record piu vecchio
                self._log.error(
                    f"Queue FULL ({self.QUEUE_MAX_SIZE}). Dropping oldest record."
                )
                try:
                    self._queue.get_nowait()  # scarta il piu vecchio
                except queue.Empty:
                    pass
                self._queue.put_nowait(normalized)
                self._send_alert(
                    f"BUFFER PIENO ({self.adapter_name}): {self.QUEUE_MAX_SIZE} record. "
                    f"Rischio perdita dati!",
                    level="ERROR",
                )

            # Warning pre-backpressure
            qsize = self._queue.qsize()
            if qsize >= self.QUEUE_WARN_SIZE and not self._backpressure_warned:
                self._backpressure_warned = True
                self._log.warning(f"Queue high: {qsize}/{self.QUEUE_MAX_SIZE}")
                self._send_alert(
                    f"Buffer alto ({self.adapter_name}): {qsize}/{self.QUEUE_MAX_SIZE}",
                    level="WARNING",
                )
            elif qsize < self.QUEUE_WARN_SIZE // 2:
                self._backpressure_warned = False

        except ValueError as e:
            # Normalization failed (es: record None o non-dict)
            self._log.warning(f"Normalization failed: {e}")
        except Exception as e:
            self._log.error(f"process_message error: {e}")

    # ============================================================
    # Batch Writer Loop (il "battito cardiaco")
    # ============================================================

    def _batch_writer_loop(self) -> None:
        """Loop che consuma la queue e scrive in batch su DuckDB.

        Si sveglia ogni FLUSH_INTERVAL_SEC O quando ci sono BATCH_SIZE record.
        """
        self._log.info(
            f"Batch writer started (batch_size={self.BATCH_SIZE}, "
            f"interval={self.FLUSH_INTERVAL_SEC}s)"
        )

        while self._running or not self._queue.empty():
            try:
                self._flush_queue()
            except Exception as e:
                self._log.error(f"Batch writer error: {e}")

            # Aspetta: o timeout o shutdown
            self._shutdown_event.wait(self.FLUSH_INTERVAL_SEC)

        # Flush finale al shutdown
        self._flush_queue()
        self._log.info("Batch writer stopped")

    def _flush_queue(self) -> None:
        """Svuota la queue e scrive il batch su DB."""
        # Drain queue
        batch: list[dict[str, Any]] = []
        while len(batch) < self.BATCH_SIZE * 2:  # max 2x per evitare drain infinito
            try:
                record = self._queue.get_nowait()
                batch.append(record)
            except queue.Empty:
                break

        if not batch:
            return

        self._log.debug(f"Flushing {len(batch)} records to {self.target_table}")

        # Valida il batch
        valid_df = self.validator.validate_batch_and_quarantine(batch, self.target_table)
        n_rejected = len(batch) - len(valid_df)

        self.metrics.inc_validated(len(valid_df))
        if n_rejected > 0:
            self.metrics.inc_quarantined(n_rejected)
            self._log.info(f"Batch: {len(valid_df)} valid, {n_rejected} quarantined")

        if valid_df.empty:
            return

        # Scrivi in DB con retry
        self._write_to_db_with_retry(valid_df)

    # ============================================================
    # DB Writing con retry
    # ============================================================

    def _write_to_db_with_retry(self, df: pd.DataFrame) -> None:
        """Scrive il DataFrame validato in DuckDB con retry esponenziale.

        Se tutti i retry falliscono, scrive in quarantena file.
        """
        last_error: Exception | None = None

        for attempt, delay in enumerate(self.DB_RETRY_DELAYS, 1):
            try:
                self._write_to_db(df)
                self.metrics.inc_batches()
                self._log.info(
                    f"Wrote {len(df)} records to {self.target_table} "
                    f"(total: {self.metrics.records_validated_ok})"
                )
                return
            except Exception as e:
                last_error = e
                self.metrics.inc_batch_errors()
                self._log.warning(
                    f"DB write failed (attempt {attempt}/{len(self.DB_RETRY_DELAYS)}): {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

        # Tutti i retry falliti: quarantena su file
        self._log.error(
            f"DB write FAILED after {len(self.DB_RETRY_DELAYS)} retries: {last_error}. "
            f"Sending {len(df)} records to file quarantine."
        )
        self._write_to_quarantine_file(df, str(last_error))
        self._send_alert(
            f"DB WRITE FAILED ({self.adapter_name}): {len(df)} records salvati in quarantena file. "
            f"Errore: {last_error}",
            level="ERROR",
        )

    def _write_to_db(self, df: pd.DataFrame) -> None:
        """Scrive il DataFrame in DuckDB in una singola transazione.

        Le sottoclassi possono fare override per INSERT custom.
        Default: INSERT INTO target_table SELECT * FROM df.
        """
        conn = self.db.get_writer()
        # Registra il DataFrame e inserisci in una transazione
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.register("_batch_df", df)
            conn.execute(f"INSERT INTO {self.target_table} SELECT * FROM _batch_df")
            conn.execute("COMMIT")
            conn.unregister("_batch_df")
        except Exception:
            conn.execute("ROLLBACK")
            conn.unregister("_batch_df")
            raise

    # ============================================================
    # Quarantena su file
    # ============================================================

    def _write_to_quarantine_file(
        self,
        df: pd.DataFrame,
        error_reason: str,
    ) -> None:
        """Scrive record rifiutati/falliti in file JSON nella cartella quarantine.

        Path: data/quarantine/YYYY-MM-DD/HH_mm_ss_{adapter_name}.json
        """
        try:
            now = datetime.now(timezone.utc)
            day_dir = self._quarantine_dir / now.strftime("%Y-%m-%d")
            day_dir.mkdir(parents=True, exist_ok=True)

            filename = f"{now.strftime('%H_%M_%S')}_{self.adapter_name}.json"
            filepath = day_dir / filename

            quarantine_data = {
                "adapter": self.adapter_name,
                "target_table": self.target_table,
                "error_reason": error_reason,
                "timestamp_utc": now.isoformat(),
                "record_count": len(df),
                "records": json.loads(df.to_json(orient="records", date_format="iso")),
            }

            filepath.write_text(json.dumps(quarantine_data, indent=2, default=str))
            self._log.info(f"Quarantine file written: {filepath} ({len(df)} records)")

        except Exception as e:
            # Ultima spiaggia: se anche la quarantena file fallisce, logga tutto
            self._log.critical(
                f"QUARANTINE FILE WRITE FAILED: {e}. "
                f"DATA LOSS: {len(df)} records from {self.adapter_name}!"
            )

    # ============================================================
    # Telegram alert helper
    # ============================================================

    def _send_alert(self, message: str, level: str = "WARNING") -> None:
        """Invia alert Telegram con rate limiting (max 1 ogni 5 min per adapter)."""
        if self.telegram is None:
            return
        now = time.time()
        if now - self._last_alert_time < self._alert_cooldown_sec:
            return  # skip: troppo presto dall'ultimo alert
        self._last_alert_time = now
        try:
            self.telegram.send_alert(message, level)
        except Exception:
            pass

    # ============================================================
    # Stats per monitoring
    # ============================================================

    def get_stats(self) -> dict[str, Any]:
        """Ritorna metriche correnti per system_monitor."""
        stats = self.metrics.get_stats()
        stats["queue_size"] = self._queue.qsize()
        stats["adapter_name"] = self.adapter_name
        stats["target_table"] = self.target_table
        stats["running"] = self._running
        return stats
