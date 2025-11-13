# 🌎 Geo-Dispatch

**A high-performance, distributed geolocation engine designed for real-time ride-matching at scale.**

Geo-Dispatch ingests high-frequency location data from thousands of concurrent drivers via a non-blocking edge service, processes it through a fault-tolerant event stream, and serves sub-50ms geospatial queries via a dual-write caching architecture.

---

## 🏗 System Architecture

The system is designed as a decoupled microservices architecture to handle high write-throughput (ingestion) and low read-latency (matchmaking).

1.  **Ingestion Service (Rust):** A high-performance TCP edge server using **Tokio**. It handles 10,000+ persistent driver connections and acts as a producer to the event stream.
2.  **Event Bus (Kafka):** Decouples ingestion from processing, providing backpressure handling and data durability.
3.  **Processing API (Go):** Consumes location events, performs geospatial enrichment (**H3 Geohashing**), and executes a dual-write strategy.
4.  **Storage Layer:**
    * **Redis (Hot):** Stores ephemeral driver locations in a Geospatial Index for instant `$O(\log N)$` radius lookups.
    * **PostgreSQL + PostGIS (Cold):** Persistent storage for historical data, analytics, and complex geometric queries using R-Tree indexing.

## 🚀 Tech Stack

* **Edge Service:** Rust, Tokio, librdkafka
* **Backend API:** Go (Golang), Goroutines
* **Messaging:** Apache Kafka, Zookeeper
* **Database:** PostgreSQL 15 (with PostGIS extension)
* **Cache:** Redis 7 (Geospatial)
* **Algorithms:** H3 (Uber's Hexagonal Hierarchical Spatial Index)
* **Infrastructure:** Docker, Docker Compose

## ✨ Key Features

* **High-Concurrency Ingestion:** Capable of handling **10k+ concurrent TCP connections** on a single node using Rust's async I/O model.
* **Real-Time Matchmaking:** v2 API achieves **<50ms p99 latency** by leveraging in-memory Redis geospatial indexes.
* **Fault Tolerance:** Architecture is fully decoupled; a database slowdown does not block the ingestion layer.
* **Algorithmic Optimization:** Implements **H3 Geohashing (Resolution 9)** to enable scalable grid-based lookups and analytics.
* **Load Testing Suite:** Includes a custom Go-based load generator to simulate thousands of concurrent drivers performing random walks.

## 🛠️ Quick Start

### Run the Full Stack
The entire system (Zookeeper, Kafka, Postgres, Redis, Go API, Rust Ingestion) is containerized.

```bash
# Start all services
docker-compose up -d
```

### API Endpoints

| Method | Endpoint                           | Description                   | Source             |
| :---   |              :---                  |            :---               |       :---         |
| `GET`  | `/find-drivers?lat=...&lon=...`    | Nearest 5 drivers (R-Tree)    | **PostgreSQL**     |
| `GET`  | `/v2/find-drivers?lat=...&lon=...` | Nearest 5 drivers (GeoRadius) | **Redis (Cached)** |
| `GET`  | `/health` | Service health check   | API                           |

🧪 Stress Testing
To verify performance, use the included load generator to spawn a swarm of mock drivers.

cd load-test
go run main.go


📂 Project Structure
├── api-service/          # Go: Kafka consumer & HTTP API
├── ingestion-service/    # Rust: High-performance TCP ingestion
├── load-test/            # Go: Concurrent load generator
├── docker-compose.yml    # Infrastructure orchestration
└── README.md             # Documentation
