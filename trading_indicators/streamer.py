import asyncio
from collections import defaultdict

from gql import gql
from gql.transport.websockets import WebsocketsTransport

from .indicators import update_rsi_state, update_vwap_state, make_default_indicator_state


async def _run_stream_async(token: str, rsi_period: int = 14, vwap_period: int = 20, duration_seconds: int = 100):
    transport = WebsocketsTransport(
        url=f"wss://streaming.bitquery.io/graphql?token={token}",
        headers={"Sec-WebSocket-Protocol": "graphql-ws"},
    )

    await transport.connect()
    print("Connected")

    query = gql(
        """
        subscription {
  Trading {
    Tokens(where: {Interval: {Time: {Duration: {eq: 1}}}}) {
      Token {
        Address
        Id
        IsNative
        Name
        Network
        Name
        Symbol
        TokenId
      }
      Block {
        Date
        Time
        Timestamp
      }
      Interval {
        Time {
          Start
          Duration
          End
        }
      }
      Volume {
        Base
        Quote
        Usd
      }
      Price {
        IsQuotedInUsd
        Ohlc {
          Close
          High
          Low
          Open
        }
        Average {
          ExponentialMoving
          Mean
          SimpleMoving
          WeightedSimpleMoving
        }
      }
    }
  }
}

        """
    )

    indicator_state_by_address = defaultdict(make_default_indicator_state)
    events_queue: asyncio.Queue = asyncio.Queue(maxsize=5000)

    async def stream_producer():
        try:
            async for result in transport.subscribe(query):
                payload = result.data
                trading = payload.get("Trading") if payload else None
                if not trading:
                    continue
                tokens = trading.get("Tokens", [])
                for entry in tokens:
                    try:
                        token = entry.get("Token", {})
                        address = (
                            token.get("Address") or token.get("Id") or token.get("TokenId") or "UNKNOWN"
                        )
                        ohlc = entry.get("Price", {}).get("Ohlc", {})
                        average = entry.get("Price", {}).get("Average", {})
                        volume_data = entry.get("Volume", {})

                        close = ohlc.get("Close")
                        simple_moving = average.get("SimpleMoving")
                        exponential_moving = average.get("ExponentialMoving")
                        weighted_simple_moving = average.get("WeightedSimpleMoving")

                        volume = (
                            volume_data.get("Base") or volume_data.get("Quote") or volume_data.get("Usd") or 1.0
                        )

                        if close is None:
                            continue
                        interval = entry.get("Interval", {}).get("Time", {})
                        end_time = interval.get("End") or interval.get("Start")
                        event = {
                            "address": address,
                            "close": float(close),
                            "time": end_time,
                            "simple_moving": simple_moving,
                            "exponential_moving": exponential_moving,
                            "weighted_simple_moving": weighted_simple_moving,
                            "volume": float(volume),
                        }
                        try:
                            events_queue.put_nowait(event)
                        except asyncio.QueueFull:
                            pass
                    except Exception:
                        continue
        except asyncio.CancelledError:
            return

    async def indicator_consumer():
        try:
            while True:
                event = await events_queue.get()
                try:
                    address = event["address"]
                    simple_moving = event["simple_moving"]
                    exponential_moving = event["exponential_moving"]
                    weighted_simple_moving = event["weighted_simple_moving"]
                    close_value = event["close"]
                    volume = event["volume"]
                    ts = event["time"]

                    state = indicator_state_by_address[address]

                    if state["prev_close"] is None:
                        state["prev_close"] = close_value
                        continue

                    rsi = update_rsi_state(state, close_value, rsi_period)
                    vwap = update_vwap_state(state, close_value, volume, vwap_period)

                    if rsi is not None:
                        print(f"Address {address}")
                        print(f"Simple Moving Average {simple_moving}")
                        print(f"Exponential Moving Average {exponential_moving}")
                        print(f"Weighted Simple Moving Average {weighted_simple_moving}")
                        print(f"VWAP[{vwap_period}] {vwap:.6f}")
                        print(f"RSI[{rsi_period}] {rsi:.3f}  Close: {close_value}  Time: {ts}")
                        print("-" * 50)
                finally:
                    events_queue.task_done()
        except asyncio.CancelledError:
            return

    consumer_task = asyncio.create_task(indicator_consumer())
    try:
        await asyncio.wait_for(stream_producer(), timeout=duration_seconds)
    except asyncio.TimeoutError:
        print(f"Stopping subscription after {duration_seconds} seconds.")
    finally:
        await events_queue.join()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    await transport.close()
    print("Transport closed")


def run_stream(token: str, rsi_period: int = 14, vwap_period: int = 20, duration_seconds: int = 100):
    asyncio.run(_run_stream_async(token, rsi_period, vwap_period, duration_seconds))


