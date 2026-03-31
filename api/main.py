import uuid
import logging
from datetime import datetime
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, BackgroundTasks
import full_snapshot
import incremental_load
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
 
app = FastAPI(
    title="NYC Taxi ETL API",
    description="API для управления ETL пайплайном NYC Yellow Taxi",
    version="1.0.0")
 
tasks: Dict[str, Any] = {}
executor = ThreadPoolExecutor(max_workers=2)

def run_etl_task(task_id: str, etl_type: str):
    tasks[task_id]["status"] = "running"
    tasks[task_id]["started_at"] = datetime.now().isoformat()
    log.info(f"[{task_id}] Запускаем {etl_type}...")
 
    try:
        if etl_type == "full":
            result = full_snapshot.run()
        else:
            result = incremental_load.run()
 
        tasks[task_id]["status"] = "success"
        tasks[task_id]["result"] = result
 
    except Exception as e:
        log.error(f"[{task_id}] Ошибка: {e}")
        tasks[task_id]["status"] = "failed"
        tasks[task_id]["error"] = str(e)
 
    finally:
        tasks[task_id]["finished_at"] = datetime.now().isoformat()
        log.info(f"[{task_id}] Завершён со статусом: {tasks[task_id]['status']}")
 
@app.post("/etl/full", summary="Запуск Full Snapshot ETL")
async def run_full_snapshot(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "task_id": task_id,
        "type": "full_snapshot",
        "status": "pending",
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,}
    background_tasks.add_task(run_etl_task, task_id, "full")
    log.info(f"Создана задача full_snapshot: {task_id}")
    return {"task_id": task_id, "status": "pending", "message": "Full Snapshot запущен"}
 
 
@app.post("/etl/incremental", summary="Запуск Incremental Load ETL")
async def run_incremental_load(background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())
    tasks[task_id] = {
        "task_id": task_id,
        "type": "incremental_load",
        "status": "pending",
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,}
    background_tasks.add_task(run_etl_task, task_id, "incremental")
    log.info(f"Создана задача incremental_load: {task_id}")
    return {"task_id": task_id, "status": "pending", "message": "Incremental Load запущен"}
 
 
@app.get("/etl/status/{task_id}", summary="Статус задачи")
async def get_task_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail=f"Задача {task_id} не найдена")
    return tasks[task_id]
 
 
@app.get("/etl/history", summary="История запусков")
async def get_history():
    history = sorted(
        tasks.values(),
        key=lambda x: x["started_at"] or "",
        reverse=True)
    return {
        "total": len(history),
        "tasks": history}
 
@app.get("/", summary="Проверка работоспособности API")
async def health_check():
    return {"status": "ok", "message": "NYC Taxi ETL API работает"}