import asyncio
from collections import defaultdict

from gql import gql
from gql.transport.websockets import WebsocketsTransport
import config

async def main():
    transport = WebsocketsTransport(
        url="wss://streaming.bitquery.io/graphql?token="+config.oauth_token,
        headers={"Sec-WebSocket-Protocol": "graphql-ws"})

    # Use `/eap` instead of `/graphql` if you are using chains on EAP endpoint
    await transport.connect()
    print("Connected")

    # Define the subscription query
    query = gql("""
        subscription {
  Trading {
    Tokens(
      where: {Token: {Network: {is: "Solana"}}, Interval: {Time: {Duration: {eq: 1}}}}
    ) {
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

    """)

    rsi_period = 14
    # Per-address RSI state using Wilder's smoothing
    rsi_state_by_address = defaultdict(lambda: {
        "prev_close": None,
        "sum_gain": 0.0,
        "sum_loss": 0.0,
        "warmup_count": 0,
        "avg_gain": None,
        "avg_loss": None,
        "initialized": False,
    })

    # Bounded queue to decouple stream consumption from RSI calculation
    events_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

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
                        address = token.get("Address") or token.get("Id") or token.get("TokenId") or "UNKNOWN"
                        ohlc = entry.get("Price", {}).get("Ohlc", {})
                        Average=entry.get("Price", {}).get("Average", {})
                        close = ohlc.get("Close")
                        simple_moving = Average.get("SimpleMoving")
                        exponential_moving = Average.get("ExponentialMoving")
                        weighted_simple_moving = Average.get("WeightedSimpleMoving")
                        if close is None:
                            continue
                        interval = entry.get("Interval", {}).get("Time", {})
                        end_time = interval.get("End") or interval.get("Start")
                        event = {"address": address, "close": float(close), "time": end_time, "simple_moving": simple_moving, "exponential_moving": exponential_moving, "weighted_simple_moving": weighted_simple_moving}
                        try:
                            events_queue.put_nowait(event)
                        except asyncio.QueueFull:
                            # Drop newest on overflow to keep producer non-blocking
                            pass
                    except Exception:
                        continue
        except asyncio.CancelledError:
            # Graceful exit on cancellation
            return

    async def rsi_consumer():
        try:
            while True:
                event = await events_queue.get()
                try:
                    address = event["address"]
                    simple_moving = event["simple_moving"]
                    exponential_moving = event["exponential_moving"]
                    weighted_simple_moving = event["weighted_simple_moving"]
                    close_value = event["close"]
                    ts = event["time"]
                    state = rsi_state_by_address[address]
                    if state["prev_close"] is None:
                        state["prev_close"] = close_value
                        continue
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
                    if state["initialized"]:
                        avg_gain = state["avg_gain"]
                        avg_loss = state["avg_loss"]
                        if avg_loss == 0:
                            rsi = 100.0 if avg_gain > 0 else 50.0
                        else:
                            rs = avg_gain / avg_loss
                            rsi = 100.0 - (100.0 / (1.0 + rs))
                        print(f"Address {address}")
                        print(f"Simple Moving Average {simple_moving}")
                        print(f"Exponential Moving Average {exponential_moving}")
                        print(f"Weighted Simple Moving Average {weighted_simple_moving}")
                        print(f"RSI[{rsi_period}] {rsi:.3f}  Close: {close_value}  Time: {ts}")
                finally:
                    events_queue.task_done()
        except asyncio.CancelledError:
            return

    # Run the subscription and stop after 100 seconds
    consumer_task = asyncio.create_task(rsi_consumer())
    try:
        await asyncio.wait_for(stream_producer(), timeout=100)
    except asyncio.TimeoutError:
        print("Stopping subscription after 100 seconds.")
    finally:
        # Drain remaining events then stop consumer
        await events_queue.join()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    # Close the connection
    await transport.close()
    print("Transport closed")


# Run the asyncio event loop
asyncio.run(main())

