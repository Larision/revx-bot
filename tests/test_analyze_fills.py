from decimal import Decimal
import unittest

from analyze_fills import detect_steps_by_day, pair_fills


def fill(ts, side, price, quantity="1"):
    return {
        "ts": ts,
        "side": side,
        "price": Decimal(price),
        "quantity": Decimal(quantity),
    }


class PairFillsTests(unittest.TestCase):
    def test_does_not_pair_a_sell_with_an_old_buy_from_another_step_regime(self):
        fills = [
            fill("2026-10-21 10:00:00", "buy", "100"),
            fill("2026-10-21 10:01:00", "sell", "110"),
            fill("2026-10-21 10:02:00", "buy", "200"),
            fill("2026-10-21 10:03:00", "sell", "400"),
        ]

        pairs, open_buys, open_sells = pair_fills(
            fills,
            Decimal("10"),
            {"2026-10-21": Decimal("10")},
        )

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["step_usdc"], "10.00")
        self.assertEqual(len(open_buys), 1)
        self.assertEqual(open_buys[0]["price"], Decimal("200"))
        self.assertEqual(len(open_sells), 1)
        self.assertEqual(open_sells[0]["price"], Decimal("400"))
        self.assertEqual(open_sells[0]["quantity"], Decimal("1"))

    def test_uses_the_day_step_instead_of_the_current_step(self):
        fills = [
            fill("2026-04-18 10:00:00", "buy", "100"),
            fill("2026-04-18 10:01:00", "sell", "115"),
        ]

        pairs, _, open_sells = pair_fills(
            fills,
            Decimal("10"),
            {"2026-04-18": Decimal("15")},
        )

        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["step_base"], "15.00")
        self.assertEqual(open_sells, [])

    def test_detect_steps_by_day_keeps_previous_step_when_day_is_inconclusive(self):
        fills = [
            fill("2026-10-20 10:00:00", "buy", "100"),
            fill("2026-10-20 10:01:00", "buy", "110"),
            fill("2026-10-20 10:02:00", "buy", "120"),
            fill("2026-10-21 10:00:00", "sell", "130"),
        ]

        steps = detect_steps_by_day(fills, Decimal("5"))

        self.assertEqual(steps["2026-10-20"], Decimal("10.00"))
        self.assertEqual(steps["2026-10-21"], Decimal("10.00"))
