import sys
import types
import unittest
from decimal import Decimal
from unittest.mock import patch

api_stub = types.ModuleType("api")


def _price_key(price: Decimal) -> str:
    return format(Decimal(str(price)).quantize(Decimal("0.01")), "f")


def _parse_balances(_balances_resp):
    return Decimal("0"), Decimal("0")


setattr(api_stub, "_parse_balances", _parse_balances)
setattr(api_stub, "_price_key", _price_key)
setattr(api_stub, "cancel_all_orders", lambda: ({}, []))
setattr(api_stub, "cancel_order", lambda _order_id: ({"status_code": 204}, []))
setattr(api_stub, "check_balances_for_grid", lambda *_args, **_kwargs: (True, []))
setattr(
    api_stub,
    "fmt_amount",
    lambda value: format(Decimal(str(value)).normalize(), "f").rstrip("0").rstrip(".") or "0",
)
setattr(api_stub, "get_active_orders", lambda: ({}, []))
setattr(api_stub, "get_all_balances", lambda: ({"balances": []}, []))
setattr(api_stub, "get_current_price", lambda: (Decimal("100"), []))
setattr(api_stub, "get_historical_orders", lambda *_args, **_kwargs: ({}, []))
setattr(api_stub, "get_order_by_id", lambda _order_id: ({}, []))
setattr(api_stub, "place_order", lambda *_args, **_kwargs: (None, []))
setattr(api_stub, "replace_order", lambda *_args, **_kwargs: (None, []))
sys.modules.setdefault("api", api_stub)

http_client_stub = types.ModuleType("http_client")
setattr(http_client_stub, "send_request", lambda *_args, **_kwargs: ({}, []))
sys.modules.setdefault("http_client", http_client_stub)

tax_fifo_stub = types.ModuleType("tax_fifo")
setattr(tax_fifo_stub, "record_tax_fill", lambda **_kwargs: None)
sys.modules.setdefault("tax_fifo", tax_fifo_stub)

from engine import GridEngine


class GridLevelHoleTests(unittest.TestCase):
    def _engine(self) -> GridEngine:
        engine = GridEngine(
            levels_below=1,
            levels_above=1,
            step_percent=Decimal("0.10"),
            base_size=Decimal("0.01"),
            initial_price=Decimal("100"),
        )
        with engine._state_lock:
            engine.center_price = Decimal("100")
            engine.step = Decimal("10")
            engine.base_step = Decimal("10")
            engine.levels = [Decimal("90"), Decimal("100"), Decimal("110")]
            engine.active_orders = {}
        return engine

    def test_cancel_order_by_key_keeps_level_empty_by_default(self) -> None:
        engine = self._engine()
        with engine._state_lock:
            engine.active_orders["90.00"] = {
                "side": "buy",
                "order_id": "order-90",
                "price": Decimal("90"),
                "placed_at": 1.0,
                "size": Decimal("0.01"),
            }

        with patch.object(engine, "save_state", return_value=True):
            ok, _logs, error_msg = engine.cancel_order_by_key("90.00", expected_order_id="order-90")

        self.assertTrue(ok)
        self.assertIsNone(error_msg)
        self.assertNotIn("90.00", engine.active_orders)
        self.assertIn(Decimal("90"), engine.levels)

    def test_manual_order_at_new_price_adds_visible_grid_level(self) -> None:
        engine = self._engine()

        with (
            patch("engine.api_place_order", return_value=("order-120", [])),
            patch.object(engine, "save_state", return_value=True),
        ):
            order_id, _logs, error_msg = engine.place_manual_order(
                Decimal("120"),
                "sell",
                Decimal("0.01"),
            )

        self.assertEqual(order_id, "order-120")
        self.assertIsNone(error_msg)
        self.assertIn("120.00", engine.active_orders)
        self.assertIn(Decimal("120"), engine.levels)


if __name__ == "__main__":
    unittest.main()
