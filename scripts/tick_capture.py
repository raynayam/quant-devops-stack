import asyncio, json, websockets, psycopg2
from datetime import datetime

DB_CONN = psycopg2.connect("dbname=trading user=trader password=tradingpass host=timescaledb")

async def capture():
    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(uri) as ws:
        print("Connected to Binance stream...")
        while True:
            msg = json.loads(await ws.recv())
            ts = datetime.fromtimestamp(msg['T']/1000)
            price = float(msg['p'])
            volume = float(msg['q'])
            with DB_CONN.cursor() as cur:
                cur.execute(
                    "INSERT INTO ticks (symbol, ts, price, volume) VALUES (%s, %s, %s, %s)",
                    ('BTCUSDT', ts, price, volume)
                )
            DB_CONN.commit()

if __name__ == "__main__":
    asyncio.run(capture())

