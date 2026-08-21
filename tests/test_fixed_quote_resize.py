import unittest
import sys
import types
from decimal import Decimal

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


class FixedQuoteResizeTests(unittest.TestCase):
    def test_virtual_sell_requires_two_price_confirmations(self) -> None:
        engine = GridEngine(
            levels_below=1,
            levels_above=1,
            step_percent=Decimal("0.10"),
            base_size=Decimal("0.01"),
            initial_price=Decimal("100"),
        )

        with engine._state_lock:
            engine.trailing_up_mode = "fixed_quote"
            engine.trailing_up_enabled = True
            engine.active_orders = {
                "110": {
                    "side": "sell",
                    "order_id": "virtual",
                    "price": Decimal("110"),
                    "placed_at": 1.0,
                    "size": Decimal("0.009"),
                },
            }

        first_filled, _first_logs = engine.detect_fills(Decimal("111"))
        second_filled, _second_logs = engine.detect_fills(Decimal("111"))

        self.assertEqual(first_filled, [])
        self.assertEqual(second_filled, ["110"])

    def test_resize_to_default_refreshes_fixed_quote_anchor_for_future_sizes(self) -> None:
        engine = GridEngine(
            levels_below=2,
            levels_above=2,
            step_percent=Decimal("0.10"),
            base_size=Decimal("0.01"),
            initial_price=Decimal("100"),
        )

        with engine._state_lock:
            engine.trailing_up_mode = "fixed_quote"
            engine.trailing_up_enabled = True
            engine.center_price = Decimal("100")
            engine.step = Decimal("10")
            engine.base_step = Decimal("10")
            engine.levels = [
                Decimal("80"),
                Decimal("90"),
                Decimal("100"),
                Decimal("110"),
                Decimal("120"),
                Decimal("130"),
                Decimal("140"),
            ]
            engine._trailing_up_fixed_quote_anchor = Decimal("100")
            engine.active_orders = {
                "140": {
                    "side": "sell",
                    "order_id": "virtual",
                    "price": Decimal("140"),
                    "placed_at": 1.0,
                    "size": Decimal("0.00714285"),
                },
            }

            old_next_size = engine._trailing_up_fixed_quote_size_locked(Decimal("150"))

        ok, _logs, error_msg, summary = engine.resize_trailing_up_fixed_quote_to_default()

        self.assertTrue(ok)
        self.assertIsNone(error_msg)
        self.assertEqual(summary["updated_state_only"], 1)

        with engine._state_lock:
            self.assertEqual(engine.active_orders["140"]["size"], Decimal("0.01"))
            self.assertEqual(engine._trailing_up_fixed_quote_anchor, Decimal("110.00"))
            self.assertEqual(summary["previous_anchor"], Decimal("100.00"))
            self.assertEqual(summary["new_anchor"], Decimal("110.00"))
            new_next_size = engine._trailing_up_fixed_quote_size_locked(Decimal("150"))

        self.assertEqual(old_next_size, Decimal("0.00666666"))
        self.assertEqual(new_next_size, Decimal("0.00733333"))

    def test_resize_preview_includes_under_anchor_fixed_quote_orders(self) -> None:
        engine = GridEngine(
            levels_below=2,
            levels_above=2,
            step_percent=Decimal("0.10"),
            base_size=Decimal("0.01"),
            initial_price=Decimal("100"),
        )

        with engine._state_lock:
            engine.trailing_up_mode = "fixed_quote"
            engine.trailing_up_enabled = True
            engine._trailing_up_fixed_quote_anchor = Decimal("100")
            engine.active_orders = {
                "90": {
                    "side": "buy",
                    "order_id": "buy-under-anchor",
                    "price": Decimal("90"),
                    "placed_at": 1.0,
                    "size": Decimal("0.009"),
                },
                "110": {
                    "side": "sell",
                    "order_id": "sell-over-anchor",
                    "price": Decimal("110"),
                    "placed_at": 1.0,
                    "size": Decimal("0.009"),
                },
                "120": {
                    "side": "sell",
                    "order_id": "already-default",
                    "price": Decimal("120"),
                    "placed_at": 1.0,
                    "size": Decimal("0.01"),
                },
            }

        preview = engine.preview_resize_trailing_up_fixed_quote_to_default()
        real_order_ids = {row["order_id"] for row in preview["real_orders"]}

        self.assertEqual(real_order_ids, {"buy-under-anchor", "sell-over-anchor"})
        self.assertEqual(preview["required_usdc"], Decimal("0.09"))
        self.assertEqual(preview["required_btc"], Decimal("0.001"))


if __name__ == "__main__":
    unittest.main()
