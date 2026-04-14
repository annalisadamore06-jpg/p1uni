"""
DuckDB Database Manager - Singleton Thread-Safe

SPECIFICHE PER QWEN:
- Pattern Singleton: una sola istanza per processo
- DUE modalita di connessione:
  1. WRITER (read-write, WAL mode) - usato SOLO dall'Ingestion thread
  2. READER (read-only) - usato da ML/Execution, riconnette ogni ciclo per dati freschi
- Thread lock per proteggere il writer
- Path DB da config/settings.yaml
- DuckDB config: threads=32, memory_limit='110GB'

IMPORTANTE:
- DuckDB NON supporta writer concorrenti. Un solo processo puo scrivere.
- La pipeline ML DEVE aprire in read_only=True
- Usa pathlib.Path per compatibilita Windows
- Il DB ml_gold.duckdb e' 159GB con 16 tabelle e 1.94B righe
"""

import threading
from pathlib import Path
from typing import Optional, Any

import duckdb
import pandas as pd


class DatabaseManager:
    """Singleton thread-safe per gestire connessioni DuckDB.

    Usage:
        db = DatabaseManager(config)
        # Thread ingestion (unico writer):
        db.execute_write("INSERT INTO trades_live VALUES (?, ?, ?, ?, ?)", params)
        # Thread ML/Execution (reader):
        df = db.execute_read("SELECT * FROM trades_live WHERE ts_event > ?", [cutoff])
    """

    _instance: Optional["DatabaseManager"] = None
    _lock = threading.Lock()

    def __new__(cls, config: dict | None = None) -> "DatabaseManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self, config: dict | None = None) -> None:
        if self._initialized:
            return
        if config is None:
            raise ValueError("First DatabaseManager init requires config dict")

        self._db_path = Path(config["database"]["path"])
        self._threads = config["database"].get("threads", 32)
        self._memory_limit = config["database"].get("memory_limit", "110GB")

        self._writer: Optional[duckdb.DuckDBPyConnection] = None
        self._writer_lock = threading.Lock()
        self._initialized = True

    def get_writer(self) -> duckdb.DuckDBPyConnection:
        """Ritorna la connessione writer singleton (thread-locked).

        QWEN: questa connessione e' in read-write con WAL mode.
        Usala SOLO dal thread di ingestion. Mai da ML o Execution.
        """
        if self._writer is None:
            self._writer = duckdb.connect(
                str(self._db_path),
                read_only=False,
                config={
                    "threads": str(self._threads),
                    "memory_limit": self._memory_limit,
                },
            )
            # WAL mode per permettere letture concorrenti
            self._writer.execute("PRAGMA wal_autocheckpoint='1GB'")
        return self._writer

    def get_reader(self) -> duckdb.DuckDBPyConnection:
        """Ritorna una connessione per lettura.

        Per DB locale (stesso processo): usa il writer (evita conflitti di config).
        Per DB esterno read-only: apri connessione separata.
        """
        # In-process: riusa il writer per evitare
        # "Can't open with different configuration" error
        return self.get_writer()

    def execute_write(self, query: str, params: list[Any] | None = None) -> None:
        """Esegue una query di scrittura con thread lock.

        QWEN: avvolgi sempre in try/except nel caller.
        Se DuckDB da lock error, logga e riprova dopo 1 secondo.
        """
        with self._writer_lock:
            conn = self.get_writer()
            if params:
                conn.execute(query, params)
            else:
                conn.execute(query)

    def execute_read(self, query: str, params: list[Any] | None = None) -> pd.DataFrame:
        """Esegue una query di lettura e ritorna un DataFrame.

        Usa la stessa connessione del writer (in-process, thread-locked).
        """
        with self._writer_lock:
            conn = self.get_reader()
            if params:
                result = conn.execute(query, params)
            else:
                result = conn.execute(query)
            return result.fetchdf()

    def close(self) -> None:
        """Chiude la connessione writer. Chiamare al shutdown."""
        with self._writer_lock:
            if self._writer is not None:
                self._writer.close()
                self._writer = None

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (per testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance.close()
                cls._instance = None
