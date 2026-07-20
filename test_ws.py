import asyncio
import websockets

async def test():
    try:
        async with websockets.connect('wss://adaptivenetshare-signalling.onrender.com') as ws:
            print('Connected to Render!')
    except Exception as e:
        print('Failed:', e)

asyncio.run(test())
