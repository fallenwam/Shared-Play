import asyncio
import random
import yt_dlp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

app = FastAPI()

# Mount the Assets directory so the frontend can load the PNGs
app.mount("/Assets", StaticFiles(directory="Assets"), name="assets")

def get_audio_info(video_id: str):
    ydl_opts: dict[str, any] = {
        'format': 'bestaudio/best',
        'quiet': True,
        'no_warnings': True
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_id, download=False)
            if info:
                return {
                    "url": str(info.get('url')),
                    "title": str(info.get('title', 'Unknown Title')),
                    "video_id": video_id
                }
    except Exception as e:
        print(f"Extraction failed for {video_id}: {e}")
    return None

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.queue: list[dict] = []
        self.original_queue: list[dict] = []
        self.current_index: int = -1
        self.repeat_mode: str = "off"  # "off", "all", "one"
        
        self.current_time: float = 0.0
        self.is_playing: bool = False

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        
        track = self.queue[self.current_index] if 0 <= self.current_index < len(self.queue) else None
        await websocket.send_json({
            "action": "state_sync", 
            "queue": self.queue, 
            "index": self.current_index,
            "repeat": self.repeat_mode,
            "time": self.current_time,
            "is_playing": self.is_playing,
            "track": track
        })

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        dead_connections = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead_connections.append(connection)
        for dead in dead_connections:
            self.disconnect(dead)

manager = ConnectionManager()

@app.get("/")
async def serve_frontend():
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")

            if action == "add":
                video_ids = data["video_ids"]
                loop = asyncio.get_running_loop()
                
                for video_id in video_ids:
                    track_info = await loop.run_in_executor(None, get_audio_info, video_id)
                    
                    if track_info:
                        manager.original_queue.append(track_info)
                        manager.queue.append(track_info)

                        if manager.current_index == -1:
                            manager.current_index = 0
                            manager.current_time = 0.0
                            manager.is_playing = True
                            await manager.broadcast({"action": "load_direct", "track": manager.queue[0], "time": 0.0})
                        
                        await manager.broadcast({
                            "action": "queue_update", 
                            "queue": manager.queue, 
                            "index": manager.current_index,
                            "repeat": manager.repeat_mode
                        })
                    else:
                        await manager.broadcast({
                            "action": "error", 
                            "message": f"Failed to load link ending in ...{video_id[-4:]}"
                        })

            elif action == "remove":
                target_idx = data.get("index")
                if target_idx is not None and 0 <= target_idx < len(manager.queue):
                    track_to_remove = manager.queue[target_idx]
                    
                    if track_to_remove in manager.original_queue:
                        manager.original_queue.remove(track_to_remove)
                    
                    manager.queue.pop(target_idx)
                    
                    if target_idx < manager.current_index:
                        manager.current_index -= 1
                    elif target_idx == manager.current_index:
                        manager.current_time = 0.0
                        if manager.current_index < len(manager.queue):
                            await manager.broadcast({"action": "load_direct", "track": manager.queue[manager.current_index], "time": 0.0})
                        else:
                            manager.current_index = -1
                            manager.is_playing = False
                            await manager.broadcast({"action": "pause"})
                    
                    await manager.broadcast({
                        "action": "queue_update", 
                        "queue": manager.queue, 
                        "index": manager.current_index,
                        "repeat": manager.repeat_mode
                    })

            elif action == "reorder":
                old_index = data.get("old_index")
                new_index = data.get("new_index")
                
                if old_index is not None and new_index is not None:
                    if 0 <= old_index < len(manager.queue) and 0 <= new_index < len(manager.queue):
                        
                        moved_track = manager.queue.pop(old_index)
                        manager.queue.insert(new_index, moved_track)
                        
                        manager.original_queue = list(manager.queue)
                        
                        if manager.current_index == old_index:
                            manager.current_index = new_index
                        elif old_index < manager.current_index <= new_index:
                            manager.current_index -= 1
                        elif new_index <= manager.current_index < old_index:
                            manager.current_index += 1
                            
                        await manager.broadcast({
                            "action": "queue_update", 
                            "queue": manager.queue, 
                            "index": manager.current_index,
                            "repeat": manager.repeat_mode
                        })

            elif action in ["next", "auto_next", "prev"]:
                if action == "auto_next" and len(manager.queue) > 0:
                    if manager.repeat_mode == "one":
                        pass 
                    elif manager.current_index < len(manager.queue) - 1:
                        manager.current_index += 1
                    elif manager.repeat_mode == "all":
                        manager.current_index = 0
                    else:
                        manager.is_playing = False
                        await manager.broadcast({"action": "pause"})
                        continue
                        
                elif action == "next" and len(manager.queue) > 0:
                    if manager.current_index < len(manager.queue) - 1:
                        manager.current_index += 1
                    elif manager.repeat_mode == "all":
                        manager.current_index = 0
                    else:
                        continue
                        
                elif action == "prev" and manager.current_index > 0:
                    manager.current_index -= 1

                manager.current_time = 0.0
                manager.is_playing = True
                await manager.broadcast({"action": "load_direct", "track": manager.queue[manager.current_index], "time": 0.0})
                await manager.broadcast({
                    "action": "queue_update", 
                    "queue": manager.queue, 
                    "index": manager.current_index,
                    "repeat": manager.repeat_mode
                })

            elif action == "play":
                manager.is_playing = True
                await manager.broadcast(data)
                
            elif action == "pause":
                manager.is_playing = False
                await manager.broadcast(data)
                
            elif action == "seek":
                manager.current_time = data.get("time", 0.0)
                await manager.broadcast(data)
                
            elif action == "sync_time":
                manager.current_time = data.get("time", 0.0)
                
            # New Shuffle Action
            elif action == "shuffle":
                current_track = manager.queue[manager.current_index] if 0 <= manager.current_index < len(manager.queue) else None
                
                other_tracks = [t for t in manager.queue if t != current_track]
                random.shuffle(other_tracks)
                manager.queue = ([current_track] + other_tracks) if current_track else other_tracks
                
                if current_track:
                    manager.current_index = 0
                
                manager.original_queue = list(manager.queue)

                await manager.broadcast({
                    "action": "queue_update", 
                    "queue": manager.queue, 
                    "index": manager.current_index,
                    "repeat": manager.repeat_mode
                })

            elif action == "toggle_repeat":
                if manager.repeat_mode == "off":
                    manager.repeat_mode = "all"
                elif manager.repeat_mode == "all":
                    manager.repeat_mode = "one"
                else:
                    manager.repeat_mode = "off"

                await manager.broadcast({
                    "action": "queue_update", 
                    "queue": manager.queue, 
                    "index": manager.current_index,
                    "repeat": manager.repeat_mode
                })
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)