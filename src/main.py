import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from src.models import Event, PublishRequest, StatsResponse, EvenListResponse
from src.dedup_store import DedupStore
from src.consumer import AggregatorConsumer

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

dedup_store: DedupStore = None
consumer : AggregatorConsumer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global dedup_store, consumer
    db_path = os.environ.get("DEDUP_DB_PATH", "app/data/dedup.db")
    logger.info(f"Starting aggregator, DB path: {db_path}")
    
    dedup_store = DedupStore(db_path=db_path)
    consumer = AggregatorConsumer(dedup_store=dedup_store)
    await consumer.start()

    logger.info("Aggregator ready")
    yield

    logger.info("Shutting down aggregator...")
    await consumer.stop()
    dedup_store.close()

app = FastAPI(
    title="Pub-Sub Log Aggregator",
    description="idempotent consumer dengan persistent deduplication",
    lifespan=lifespan,
)

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/publish", status_code=202)
async def publish(request: PublishRequest):
    
    if not request.events:
        raise HTTPException(status_code=400, detail="Events list tidak boleh kosong")
    enqueued = 0
    for event in request.events:
        await consumer.enqueue(event)
        enqueued += 1

    logger.info(f"[Publish] {enqueued} event(s) enqueued from batch")
    return {
        "status": "accepted",
        "enqueued": enqueued,
        "message": f"{enqueued} event(s) diterimna untuk diproses",
    }

@app.get("/events")
async def get_events(topic: str = Query(..., description="Nama topic yang ingin dilihat")):
    events = dedup_store.get_events_by_topic(topic)
    return {
        "topic": topic,
        "count": len(events),
        "events": [e.model_dump() for e in events],
    }

@app.get("/stats")
async def get_stats():
    stats = consumer.get_stats()
    return stats

@app.get("/topics")
async def list_topics():
    topics = dedup_store.get_all_topics()
    return {"topics": topics, "count": len(topics)}
