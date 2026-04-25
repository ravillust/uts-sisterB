import asyncio
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
import pytest
import pytest_asyncio

os.environ["DEDUP_DB_PATH"] = ":memory:"
from src.models import Event
from src.dedup_store import DedupStore
from src.consumer import AggregatorConsumer

def make_event(topic: str = None, event_id: str = None, source: str = "test") -> Event:
    return Event(
        topic=topic,
        event_id=event_id or f"evt-{uuid.uuid4().hex[:12]}",
        timestamp=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        source=source,
        payload={"msg": "unit test event"},
    )

def make_store(path: str = ":memory:") -> DedupStore:
    return DedupStore(db_path=path)

class TestEventSchema:
    def test_valid_event(self):
        ev = make_event()
        assert ev.topic == "test.topic"
        assert ev.event_id is not None
        assert ev.source == "test"

    def test_invalid_timestamp(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError, match="ISO 8601"):
            Event(
                topic="t",
                event_id="e1",
                timestamp="not-a-date",
                source="s",
                payload={},
            )
    def test_invalid_topic_with_spaces(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            Event(
                topic="topic with spaces",
                event_id="e1",
                timestamp="2026-04-21T00:00:00Z",
                source="s",
                payload={},
            )
    def test_event_id_no_spaces(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            Event(
                topic="valid.topic",
                event_id="event id with spaces",
                timestamp="2026-04-21T00:00:00Z",
                source="s",
                payload={},
            )
    def test_iso8601_with_z_suffix(self):
        ev = Event(
            topic="t",
            event_id="e1",
            timestamp="2026-04-21T12:30:00Z",
            source="s",
            payload={},
        )
        assert "Z" in ev.timestamp

class TestDedupStore:
    def test_save_new_event(self):
        store = make_store()
        ev = make_event()
        assert store.save(ev) is True
    def test_duplicate_rejected(self):
        store = make_store()
        ev = make_event(event_id="dup-001")
        assert store.save(ev) is True
        assert store.save(ev) is False
    def test_same_event_id_different_topic(self):
        store = make_store()
        ev1 = make_event(topic="topic1", event_id="shared-id")
        ev2 = make_event(topic="topic2", event_id="shared-id")
        assert store.save(ev1) is True
        assert store.save(ev2) is True
    def test_is_duplicate_check(self):
        store = make_store()
        ev = make_event(event_id="dup-check001")
        assert store.is_duplicate(ev.topic, ev.event_id) is False
        store.save(ev)
        assert store.is_duplicate(ev.topic, ev.event_id) is True
    def test_persistence_across_instances(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            path = f.name
        try:
            store1 = DedupStore(db_path=db_path)
            ev = make_event(event_id="persist-001")
            assert store1.save(ev) is True
            store1.close()

            store2 = DedupStore(db_path=db_path)
            assert store2.is_duplicate(ev.topic, ev.event_id) is True
            assert store2.save(ev) is False
            store2.close()
        finally:
            os.unlink(db_path)
    def test_get_events_by_topic(self):
        store = make_store()
        for i in range(5):
            store.save(make_event(topic="special.topic", event_id=f"sp-{i}"))
        store.save(make_event(topic="other.topic", event_id="other-001"))

        events = store.get_events_by_topic("special.topic")
        assert len(events) == 5
        assert all(e.topic == "special.topic" for e in events)
    def test_count_unique(self):
        store = make_store()
        for i in range(10):
            store.save(make_event(event_id=f"count-{i}"))
        for i in range(3):
            store.save(make_event(event_id=f"count-{i}"))
        assert store.count_unique() == 10

class TestIdempotentConsumer:
    @pytest.fixture
    def store(self):
        return make_store()
    @pytest.fixture
    def consumer_instance(self, store):
        return AggregatorConsumer(dedup_store=store)
    
    @pytest.mark.asyncio
    async def test_consumer_processes_unique(self, consumer_instance):
        await consumer_instance.start()
        ev = make_event(event_id="unique-001")
        await consumer_instance.enqueue(ev)
        await asyncio.sleep(0.2)
        await consumer_instance.stop()

        assert consumer_instance.stats["unique_processed"] == 1
        assert consumer_instance.stats["duplicate_dropped"] == 0
    
    @pytest.mark.asyncio
    async def test_consumer_drops_duplicates(self, consumer_instance):
        await consumer_instance.start()
        ev = make_event(event_id="dup-001")
        await consumer_instance.enqueue(ev)
        await consumer_instance.enqueue(ev)
        await consumer_instance.enqueue(ev)
        await asyncio.sleep(0.3)
        await consumer_instance.stop()

        assert consumer_instance.stats["unique_processed"] == 1
        assert consumer_instance.stats["duplicate_dropped"] == 2
        assert consumer_instance.stats["received"] == 3
    @pytest.mark.asyncio
    async def test_stats_consistency(self, consumer_instance):
        await consumer_instance.start()
        unique_events = [make_event(event_id=f"stat-uniq-{i}") for i in range(10)]
        dup_events = make_event(event_id="stat-dup-001")

        for ev in unique_events:
            await consumer_instance.enqueue(ev)
        for _ in range(5):
            await consumer_instance.enqueue(dup_events)
        await asyncio.sleep(0.5)
        await consumer_instance.stop()

        stats = consumer_instance.get_stats()
        assert stats["received"] == 15
        assert stats["unique_processed"] == 11
        assert stats["duplicate_dropped"] == 4
        assert stats["received"] == stats["unique_processed"] + stats["duplicate_dropped"]

class TestStress:
    @pytest.mark.asyncio
    async def test_batch_5000_events(self):
        store = make_store()
        consumer = AggregatorConsumer(dedup_store=store)
        await consumer.start()

        total = 5000
        dup_rate = 0.25
        unique_count = int(total * (1 - dup_rate))

        unique_events = [make_event(event_id=f"stress-uniq-{i}") for i in range(unique_count)]
        duplicates = [unique_events[i % len(unique_count)] for i in range(total - unique_count)]
        all_events = unique_events + duplicates

        start = time.time()
        for ev in all_events:
            await consumer.enqueue(ev)
        await consumer.queue.join()
        elapsed = time.time() - start

        await consumer.stop()

        assert elapsed < 30, f"Stress test terlalu lambat: {elapsed:.2f}s"
        assert consumer.stats["unique_processed"] == unique_count
        assert consumer.stats["duplicate_dropped"] == total - unique_count
        assert consumer.stats["received"] == total
        print(f"\n[STRESS] {total} events dalam {elapsed:.2f}s 9{total/elapsed:.0f} ev/s")