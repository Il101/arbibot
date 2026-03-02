import asyncio
import ccxt.async_support as ccxt

async def main():
    try:
        ex = ccxt.bingx()
        ob = await ex.fetch_order_book('BTC/USDT:USDT', 20)
        print("BingX Orderbook fetch success:", len(ob['bids']), len(ob['asks']))
        await ex.close()
    except Exception as e:
        print("Error BingX:", e)

    try:
        ex = ccxt.bybit()
        ob = await ex.fetch_order_book('BTC/USDT:USDT', 20)
        print("Bybit Orderbook fetch success:", len(ob['bids']), len(ob['asks']))
        await ex.close()
    except Exception as e:
        print("Error Bybit:", e)

if __name__ == '__main__':
    asyncio.run(main())
