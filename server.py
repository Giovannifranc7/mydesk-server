import asyncio
import json
import websockets

clients = {}  # { id: host_websocket }
viewers = {}  # { viewer_websocket: host_id }

async def handler(websocket):
    current_id = None
    try:
        async for message in websocket:
            if isinstance(message, bytes):
                for v_ws, h_id in viewers.items():
                    if clients.get(h_id) == websocket:
                        await v_ws.send(message)
                continue

            data = json.loads(message)
            role = data.get("role")

            if role == "agent":
                current_id = data.get("id")
                clients[current_id] = websocket
                print(f"[Host Registrado] ID: {current_id}")
            elif role == "viewer":
                target_id = data.get("id")
                password = data.get("password")
                target_ws = clients.get(target_id)
                if not target_ws:
                    await websocket.send(json.dumps({"type": "error", "msg": "ID remoto não encontrado ou offline."}))
                    continue
                viewers[websocket] = target_id
                await target_ws.send(json.dumps({"type": "viewer_hello", "password": password}))
            else:
                target_id = viewers.get(websocket)
                if target_id:
                    target_ws = clients.get(target_id)
                    if target_ws:
                        await target_ws.send(message)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if current_id and clients.get(current_id) == websocket:
            del clients[current_id]
            print(f"[Host Desconectado] ID: {current_id}")
        for v_ws, h_id in list(viewers.items()):
            if v_ws == websocket:
                target_ws = clients.get(h_id)
                if target_ws:
                    await target_ws.send(json.dumps({"type": "viewer_left"}))
                del viewers[v_ws]

async def main():
    async with websockets.serve(handler, "0.0.0.0", 8765):
        print("Servidor WebSocket rodando na porta 8765...")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())