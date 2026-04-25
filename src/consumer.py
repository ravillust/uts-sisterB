import asyncio
import logging
import time
from src.models import Event
from src.dedup_store import DedupStore

logger = logging.getLogger(__name__)

class AggregatorConsumer:
    def __init__(self, dedup_store: DedupStore, queue_maxsize: int = 100_000):
        self.dedup_store = dedup_store
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_maxsize)

        # Initialize stats from database (persistent across restarts)
        total_received = self.dedup_store.get_total_event_count()
        total_unique = self.dedup_store.get_unique_event_count()
        total_duplicates = self.dedup_store.get_duplicate_count()

        self.stats = {
            "received": total_received,
            "unique_processed": total_unique,
            "duplicate_dropped": total_duplicates,
            "start_time": time.time(),
        }

        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("AggregatorConsumer started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("AggregatorConsumer stopped")

    async def enqueue(self, event: Event) -> None:
        self.stats["received"] += 1
        await self.queue.put(event)

    async def _consume_loop(self) -> None:
        logger.info("Consumer loop started, waiting for events...")
        while self._running:
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                await self._process_event(event)
                self.queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in consume loop: {e}", exc_info=True)

    async def _process_event(self, event: Event) -> None:
        saved = await asyncio.get_event_loop().run_in_executor(
            None, self.dedup_store.save, event
        )
        if saved:
            self.stats["unique_processed"] += 1
            logger.debug(
                f"[PROCESSED] topic={event.topic!r} event_id={event.event_id!r}"
            )
        else:
            self.stats["duplicate_dropped"] += 1
            self.dedup_store.increment_duplicate_count()
    
    def get_stats(self) -> dict:
        topics = self.dedup_store.get_all_topics()
        uptime = time.time() - self.stats["start_time"]
        return {
            "received": self.stats["received"],
            "unique_processed": self.stats["unique_processed"],
            "duplicate_dropped": self.stats["duplicate_dropped"],
            "topics": topics,
            "uptime_seconds": round(uptime, 2),
        }
