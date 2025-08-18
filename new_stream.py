from trading_indicators import load_token, run_stream


def main():
    token = load_token()
    run_stream(token=token, rsi_period=14, vwap_period=20, duration_seconds=100)


if __name__ == "__main__":
    main()

