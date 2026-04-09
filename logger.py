import logging
from typing import List, Optional

from config import LOG_PATH
from types_ import LogEntry


fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# Logger principal: consola + fichero
logger = logging.getLogger("grid_engine_v1.0")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(ch)

fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

# Logger solo-fichero: para show_grid_preview y otros sitios
# donde el print ya cubre la consola y no queremos duplicados
file_logger = logging.getLogger("grid_engine_v1.0.file")
file_logger.setLevel(logging.INFO)
file_logger.addHandler(fh)
file_logger.propagate = False


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
