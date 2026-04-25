# Pub-Sub Log Aggregator
### UTS Sistem Terdistribusi dan Parallel

Layanan log aggregator berbasis **Pub-Sub pattern** dengan **idempotent consumer** dan **persistent deduplication**, dibangun menggunakan Python + FastAPI + SQLite.

---

## Arsitektur Singkat

```
Publisher(s)  →  POST /publish  →  asyncio.Queue  →  Consumer (async)
                                                          ↓
                                                    DedupStore (SQLite)
                                                          ↓
                                              GET /events, GET /stats
```

---

## Cara Build & Run

### Docker (Wajib)

```bash
# Build image
docker build -t uts-aggregator .

# Run container (dengan persistent volume untuk dedup store)
docker run -p 8080:8080 \
  -v aggregator-data:/app/data \
  uts-aggregator

# Cek health
curl http://localhost:8080/health
```

### Docker Compose (Bonus)

```bash
# Jalankan aggregator + publisher sekaligus
docker compose up --build

# Hanya aggregator (tanpa publisher simulator)
docker compose up aggregator
```

### Lokal (Development)

```bash
pip install -r requirements.txt
DEDUP_DB_PATH=./data/dedup.db PYTHONPATH=. \
  uvicorn src.main:app --port 8080 --reload
```

---

## Endpoint API

| Method | Path | Deskripsi |
|--------|------|-----------|
| `POST` | `/publish` | Terima batch/single event |
| `GET` | `/events?topic=<name>` | Daftar event unik per topic |
| `GET` | `/stats` | Statistik aggregator |
| `GET` | `/topics` | Daftar semua topic aktif |
| `GET` | `/health` | Health check |

### Contoh: Publish Event

```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "topic": "auth.login",
        "event_id": "evt-abc123",
        "timestamp": "2024-01-15T10:30:00Z",
        "source": "api-gateway",
        "payload": {"user_id": 42, "status": "success"}
      }
    ]
  }'
```

### Contoh: Simulasi At-Least-Once (Kirim Duplikat)

```bash
# Kirim event yang sama 3 kali — hanya diproses 1 kali
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/publish \
    -H "Content-Type: application/json" \
    -d '{"events": [{"topic": "test", "event_id": "dup-001",
         "timestamp": "2024-01-01T00:00:00Z", "source": "sim", "payload": {}}]}'
done

# Cek stats → duplicate_dropped: 2
curl http://localhost:8080/stats
```

### Contoh: Get Events

```bash
curl "http://localhost:8080/events?topic=auth.login"
```

### Contoh: Get Stats

```bash
curl http://localhost:8080/stats
# Response:
# {
#   "received": 100,
#   "unique_processed": 75,
#   "duplicate_dropped": 25,
#   "topics": ["auth.login", "payment.processed"],
#   "uptime_seconds": 42.3
# }
```

---

## Menjalankan Unit Tests

```bash
# Di lokal
PYTHONPATH=. DEDUP_DB_PATH=":memory:" \
  python -m pytest tests/ -v

# Di dalam container
docker run --rm uts-aggregator \
  python -m pytest tests/ -v -p no:cacheprovider
```

**Hasil: 16 tests, semua PASSED**

| Test | Deskripsi |
|------|-----------|
| `test_valid_event` | Schema event valid |
| `test_invalid_timestamp` | Timestamp non-ISO8601 ditolak |
| `test_invalid_topic_with_space` | Topic dengan karakter invalid ditolak |
| `test_event_id_no_space` | event_id dengan spasi ditolak |
| `test_iso8601_with_z_suffix` | Timestamp Z-suffix diterima |
| `test_save_new_event` | Event baru tersimpan |
| `test_duplicate_rejected` | Event duplikat di-drop |
| `test_same_event_id_different_topic` | event_id sama ≠ duplikat jika topic beda |
| `test_is_duplicate_check` | Cek duplikat konsisten |
| `test_persistence_across_instances` | Dedup tahan restart |
| `test_get_events_by_topic` | Query per topic benar |
| `test_count_unique` | Count hanya event unik |
| `test_consumer_processes_unique` | Consumer memproses event unik |
| `test_consumer_drops_duplicate` | Consumer drop duplikat |
| `test_stats_consistency` | received == unique + dropped |
| `test_batch_5000_events` | 5000 events < 30 detik ✓ |

---

## Publisher Simulator

```bash
# Demo mode (lokal)
PYTHONPATH=. python -m src.publisher --mode demo

# Stress test: 5000 events, 25% duplikat
PYTHONPATH=. python -m src.publisher --mode stress \
  --count 5000 --dup-rate 0.25
```

---

## Asumsi Desain

1. **Ordering**: Total ordering tidak diperlukan untuk log aggregator. Event dari source berbeda tidak perlu diurutkan global. Ordering per-topic adalah cukup, dan SQLite menyimpan berdasarkan waktu insert.

2. **Dedup scope**: Pasangan `(topic, event_id)` dijadikan primary key. Satu `event_id` bisa muncul di topic berbeda dan itu bukan duplikat.

3. **At-least-once delivery**: Publisher boleh mengirim event yang sama berkali-kali (simulasi retry). Consumer yang bertanggung jawab memastikan idempotency.

4. **Persistensi**: SQLite dengan WAL mode memastikan data tidak hilang saat container restart, selama volume di-mount.

5. **Stats in-memory**: Counter (`received`, `unique_processed`, `duplicate_dropped`) di-reset saat restart, tapi `unique_processed` akan konsisten dengan data di SQLite setelah consumer flush.

---

## Environment Variables

| Variable | Default | Deskripsi |
|----------|---------|-----------|
| `DEDUP_DB_PATH` | `/app/data/dedup.db` | Path SQLite database |
| `LOG_LEVEL` | `INFO` | Level logging (DEBUG/INFO/WARNING) |

---

## Video Demo

🎬 **[Link YouTube Demo](https://youtu.be/AJy16TcPlbg)**

Durasi: 5–8 menit, mendemonstrasikan:
- Build image dan run container
- Kirim event duplikat (at-least-once simulation)
- Verifikasi GET /stats dan GET /events
- Restart container → dedup store masih efektif
- Ringkasan arsitektur

---

## Struktur Repository

```
uts-aggregator/
├── src/
│   ├── __init__.py
│   ├── main.py          # FastAPI app & endpoints
│   ├── models.py        # Pydantic schema Event
│   ├── dedup_store.py   # SQLite dedup store
│   ├── consumer.py      # Async idempotent consumer
│   └── publisher.py     # Publisher simulator
├── tests/
│   ├── __init__.py
│   └── test_aggregator.py  # 16 unit tests
├── Dockerfile
├── docker-compose.yml   # Bonus: dua service terpisah
├── requirements.txt
├── pyproject.toml
├── README.md
└── report.md
```