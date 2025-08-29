from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fire_processor import FireProcessor
from scheduler import scheduler_instance
import time
from datetime import datetime

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

fire_cache = {
    "data": None,
    "timestamp": None,
    "processing": False
}

@app.on_event("startup")
async def startup_event():
    scheduler_instance.start_in_background()
    print("Fire scheduler started")

@app.get("/")
async def root():
    return {"message": "API Procesamiento de Incendios", "status": "ok"}

@app.post("/process-fires")
async def process_fires():
    if fire_cache["processing"]:
        return {
            "success": False,
            "message": "Ya se está procesando incendios. Espera unos minutos.",
            "processing": True
        }
    
    fire_cache["processing"] = True
    
    try:
        processor = FireProcessor()
        result = processor.process_all()
        
        fire_cache["data"] = result
        fire_cache["timestamp"] = time.time()
        fire_cache["processing"] = False
        
        return result
        
    except Exception as e:
        fire_cache["processing"] = False
        return {"success": False, "error": str(e)}

@app.get("/fires-cache")
async def get_fires_cache():
    if fire_cache["data"] and fire_cache["timestamp"]:
        age_minutes = (time.time() - fire_cache["timestamp"]) / 60
        return {
            "success": True,
            "from_cache": True,
            "cache_age_minutes": round(age_minutes, 1),
            **fire_cache["data"]
        }
    else:
        return {
            "success": False,
            "message": "No hay cache de incendios disponible",
            "suggestion": "Ejecuta /process-fires primero"
        }

@app.get("/fires-status")
async def fires_status():
    if fire_cache["timestamp"]:
        age_minutes = (time.time() - fire_cache["timestamp"]) / 60
        return {
            "cache_available": bool(fire_cache["data"]),
            "cache_age_minutes": round(age_minutes, 1),
            "processing": fire_cache["processing"],
            "last_update": datetime.fromtimestamp(fire_cache["timestamp"]).strftime("%Y-%m-%d %H:%M:%S") if fire_cache["timestamp"] else None,
            "stats": fire_cache["data"].get("stats") if fire_cache["data"] else None
        }
    else:
        return {
            "cache_available": False,
            "processing": fire_cache["processing"],
            "message": "No hay procesamiento previo"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
