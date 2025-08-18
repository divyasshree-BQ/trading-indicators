import argparse

from .config import load_token
from .streamer import run_stream


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trading-indicators",
        description="Stream RSI and VWAP indicators from Bitquery",
    )
    parser.add_argument("--token", help="Bitquery API token (overrides env/config)")
    parser.add_argument("--config", help="Path to config.json containing oauth_token")
    parser.add_argument("--rsi-period", type=int, default=14, help="RSI period (default: 14)")
    parser.add_argument("--vwap-period", type=int, default=20, help="VWAP rolling window size (default: 20)")
    parser.add_argument("--duration", type=int, default=100, help="Stream duration in seconds (default: 100)")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    token = load_token(explicit_token=args.token, config_path=args.config)
    run_stream(
        token=token,
        rsi_period=args.rsi_period,
        vwap_period=args.vwap_period,
        duration_seconds=args.duration,
    )


if __name__ == "__main__":
    main()


