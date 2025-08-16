import asyncio
from coingecko_sdk import DefaultAioHttpClient
from coingecko_sdk import AsyncCoingecko
import coingecko_sdk
from dotenv import load_dotenv
import os

from datetime import datetime, timedelta, timezone

# load env vars from .env file
load_dotenv()

async def main() -> None:
    async with AsyncCoingecko(
        demo_api_key=os.environ.get("COINGECKO_DEMO_API_KEY"),
        environment="demo",
        http_client=DefaultAioHttpClient(),
    ) as client:
        
        try:
            price = await client.simple.price.get(
                vs_currencies="usd",
                ids="bitcoin",
            )
            print(price)
        except coingecko_sdk.APIConnectionError as e:
            print("The server could not be reached")
            print(e.__cause__)  # an underlying Exception, likely raised within httpx.
        except coingecko_sdk.RateLimitError as e:
            print("A 429 status code was received; we should back off a bit.")
        except coingecko_sdk.APIStatusError as e:
            print("Another non-200-range status code was received")
            print(e.status_code)
            print(e.response)

        # price = await client.coins.market_chart.get_range(
        #     id='btc',
        #     from_=(datetime.now(tz=timezone.utc) - timedelta(days=5)).timestamp(),
        #     to=datetime.now(tz=timezone.utc).timestamp(),
        #     interval='daily',
        #     vs_currency='usd',
        #     # vs_currencies="usd",
        #     # symbols="btc,eth,sol,fart,pump,bonk,mog",
        # )

        # print(price)


asyncio.run(main())