from collections import defaultdict
from typing import Dict, List


def update_rsi_state(state: dict, close_value: float, rsi_period: int):
    """Update RSI state in-place and return RSI value if initialized, else None.

    Wilder's smoothing is used. The caller should ensure prev_close is populated
    on first tick in order to skip calculation on the first value.
    """
    delta = close_value - state["prev_close"]
    gain = delta if delta > 0 else 0.0
    loss = -delta if delta < 0 else 0.0

    if not state["initialized"]:
        state["sum_gain"] += gain
        state["sum_loss"] += loss
        state["warmup_count"] += 1
        if state["warmup_count"] >= rsi_period:
            state["avg_gain"] = state["sum_gain"] / rsi_period
            state["avg_loss"] = state["sum_loss"] / rsi_period
            state["initialized"] = True
    else:
        state["avg_gain"] = ((state["avg_gain"] * (rsi_period - 1)) + gain) / rsi_period
        state["avg_loss"] = ((state["avg_loss"] * (rsi_period - 1)) + loss) / rsi_period

    state["prev_close"] = close_value

    if not state["initialized"]:
        return None

    avg_gain = state["avg_gain"]
    avg_loss = state["avg_loss"]
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def update_vwap_state(state: dict, price: float, volume: float, vwap_period: int):
    """Update VWAP state in-place and return current VWAP over a rolling window."""
    state["vwap_prices"].append(price)
    state["vwap_volumes"].append(volume)

    if len(state["vwap_prices"]) > vwap_period:
        removed_price = state["vwap_prices"].pop(0)
        removed_volume = state["vwap_volumes"].pop(0)
        state["cumulative_pv"] -= removed_price * removed_volume
        state["cumulative_volume"] -= removed_volume

    state["cumulative_pv"] += price * volume
    state["cumulative_volume"] += volume

    return (
        state["cumulative_pv"] / state["cumulative_volume"]
        if state["cumulative_volume"] > 0
        else price
    )


def make_default_indicator_state():
    return {
        "prev_close": None,
        "sum_gain": 0.0,
        "sum_loss": 0.0,
        "warmup_count": 0,
        "avg_gain": None,
        "avg_loss": None,
        "initialized": False,
        # VWAP state
        "vwap_prices": [],
        "vwap_volumes": [],
        "cumulative_pv": 0.0,  # cumulative price * volume
        "cumulative_volume": 0.0,
    }


