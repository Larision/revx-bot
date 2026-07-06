import os
from decimal import Decimal, getcontext
from pathlib import Path


VERSION: str = "v1.3"

LOG_PATH   = Path("gridbot_v1_3.log")  # fichero de log principal
FILLS_PATH = Path("fills.csv")         # registro CSV de fills confirmados
STATE_PATH = Path("grid_state.json")   # estado persistente del grid
UPDATE_RECOVER_PATH = Path(".revx_update_recover")  # marcador temporal post-update

BASE_URL: str = os.environ.get("REVX_BASE_URL", "https://revx.revolut.com")
SYMBOL: str = "BTC-USDC"  # Cambiar aquí afecta todo el programa
MAX_TRADES_HISTORY_LIMIT = 1900
WINDOW_MS = 7 *24 * 60 * 60 * 1000

DEFAULT_GRID_LEVELS_BELOW: int = 3
DEFAULT_GRID_LEVELS_ABOVE: int = 3
DEFAULT_GRID_STEPS: int = DEFAULT_GRID_LEVELS_ABOVE  # alias legacy (grid simétrico)
DEFAULT_BASE_SIZE: Decimal = Decimal("0.00008")
DEFAULT_STEP_PERCENT: Decimal = Decimal("0.002")
DEFAULT_TRAILING_UP: str = "extended"
DEFAULT_TRAILING_DOWN: str = "on"
TICK_SIZE: Decimal = Decimal("0.01")
MIN_USDC_RESERVE: Decimal = Decimal("0")  # Reserva USDC predeterminada

getcontext().prec = 28
