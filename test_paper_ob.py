import asyncio
from core.exchanges.paper import PaperExchange

class MockWS:
    def get_latest_price(self, exchange, symbol):
        return {'bid': 90000.0, 'ask': 90005.0, 'last': 90002.5, 'timestamp': 123456789}

async def check():
    ws = MockWS()
    
    # Try Bybit
    print("\n--- Testing Bybit ---")
    bybit = PaperExchange("bybit", 10000, ws)
    ob = await bybit.fetch_order_book('BTC/USDT', 5)
    print("Bids:", ob['bids'])
    print("Asks:", ob['asks'])
    await bybit.close()
    
    # Try BingX
    print("\n--- Testing BingX ---")
    bingx = PaperExchange("bingx", 10000, ws)
    ob = await bingx.fetch_order_book('BTC/USDT', 5)
    print("Bids:", ob['bids'])
    print("Asks:", ob['asks'])
    await bingx.close()

if __name__ == '__main__':
    asyncio.run(check())
