import logging
from typing import List, Optional

from config import LOG_PATH
from types_ import LogEntry


# ============================
#  Color Formatter (solo consola)
# ============================

class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[37m",     # gris
        "INFO": "\033[36m",      # cyan
        "WARNING": "\033[33m",   # amarillo
        "ERROR": "\033[31m",     # rojo
        "CRITICAL": "\033[41m",  # fondo rojo
    }
    RESET = "\033[0m"

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, "")
        record.levelname = f"{color}{levelname}{self.RESET}"
        return super().format(record)


# ============================
#  Formatters
# ============================

fmt_file = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fmt_console = ColorFormatter("%(asctime)s [%(levelname)s] %(message)s")


# ============================
#  Logger principal: consola + fichero
# ============================

logger = logging.getLogger("grid_engine_v1.2")
logger.setLevel(logging.INFO)

# Consola (con colores)
ch = logging.StreamHandler()
ch.setFormatter(fmt_console)
logger.addHandler(ch)

# Fichero (sin colores)
fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
fh.setFormatter(fmt_file)
logger.addHandler(fh)


# ============================
#  Logger solo-fichero
# ============================

file_logger = logging.getLogger("grid_engine_v1.2.file")
file_logger.setLevel(logging.INFO)
file_logger.addHandler(fh)
file_logger.propagate = False


# ============================
#  Funciones de logging
# ============================

def log_event(
    message: str,
    level: str = "info",
    collector: Optional[List[LogEntry]] = None
) -> None:
    """
    Logger unificado. Escribe en consola y fichero.
    Si se pasa collector, añade además una entrada estructurada.
    """
    getattr(logger, level)(message)
    if collector is not None:
        collector.append({"level": level, "msg": message})


def log_file(message: str, level: str = "info") -> None:
    """Escribe solo en el fichero, sin duplicar en consola."""
    getattr(file_logger, level)(message)


def log_fill(side: str, price: str, quantity: str) -> None:
    """
    Registra un fill confirmado en fills.csv.
    Columnas: timestamp, side, price, quantity, value_usd
    """
    import csv
    from decimal import Decimal as D
    from config import FILLS_PATH

    try:
        value_usd = f"{D(price) * D(quantity):.2f}"
        file_exists = FILLS_PATH.exists()
        with open(FILLS_PATH, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "side", "price", "quantity", "value_usd"])
            import time as _time
            ts = _time.strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([ts, side, price, quantity, value_usd])
    except Exception as e:
        logger.warning(f"[FILLS] Error escribiendo fills.csv: {e}")

