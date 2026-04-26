import os
from decimal import Decimal, getcontext
from pathlib import Path


VERSION: str = "v1.1"

LOG_PATH   = Path("gridbot_v1_1.log")  # fichero de log principal
FILLS_PATH = Path("fills.csv")         # registro CSV de fills confirmados
STATE_PATH = Path("grid_state.json")   # estado persistente del grid

BASE_URL: str = os.environ.get("REVX_BASE_URL", "https://revx.revolut.com")
SYMBOL: str = "BTC-USDC"  # Cambiar aquí afecta todo el programa

DEFAULT_GRID_LEVELS_BELOW: int = 3
DEFAULT_GRID_LEVELS_ABOVE: int = 3
DEFAULT_GRID_STEPS: int = DEFAULT_GRID_LEVELS_ABOVE  # alias legacy (grid simétrico)
DEFAULT_BASE_SIZE: Decimal = Decimal("0.00008")
DEFAULT_STEP_PERCENT: Decimal = Decimal("0.002")
TICK_SIZE: Decimal = Decimal("0.01")
MIN_USDC_RESERVE: Decimal = Decimal("20")  # USDC mínimo reservado — no usar en trailing down extendido

getcontext().prec = 28
