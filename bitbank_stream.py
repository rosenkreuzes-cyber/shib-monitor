import socketio
class BitbankStream:
    def __init__(self,pair,callback):
        self.pair=pair; self.callback=callback; self.sio=socketio.AsyncClient(reconnection=True,reconnection_attempts=0)
        @self.sio.event
        async def connect():
            for room in (f"depth_diff_{pair}",f"depth_whole_{pair}",f"transactions_{pair}",f"ticker_{pair}"):
                await self.sio.emit("join-room",room)
        @self.sio.on("message")
        async def message(payload):
            if isinstance(payload,dict): await self.callback(payload.get("room_name"),payload.get("message",{}))
    async def run(self):
        await self.sio.connect("https://stream.bitbank.cc",transports=["websocket"]); await self.sio.wait()
