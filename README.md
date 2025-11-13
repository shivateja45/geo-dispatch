# 🌎 Geo-Dispatch: High-Performance Geolocation Service

Geo-Dispatch is a high-performance backend system that solves the "nearest neighbor" problem for a ride-sharing platform. It's a distributed, real-time data pipeline built with Go, Rust, Kafka, Redis, and Postgres.

This project demonstrates a real-world, SDE 2-level architecture designed for high throughput and low latency.

## 🚀 Core Features

* **High-Throughput Ingestion:** A non-blocking TCP server built in **Rust** with **Tokio** to handle 10,000+ concurrent driver connections.
* **Decoupled Architecture:** A **Kafka** message queue acts as a "shock absorber," decoupling the ingestion service from the API.
* **Real-time API Service:** A **Go**-based microservice that consumes from Kafka, enriches data, and serves API requests.
* **Optimized Caching:** Implements a dual-write strategy:
    * **Redis (Cache):** Stores live driver locations in a **geospatial index** for sub-50ms API reads.
    * **PostgreSQL (DB):** Persists all data to a **PostGIS**-enabled database for accuracy and analytics.
* **Algorithmic Optimization:** Enriches incoming data with **H3 geohashes** (Uber's grid system) to enable high-performance grid-based searches.

## 🛠️ System Architecture


*(You can create this using a free tool like [Excalidraw](https://excalidraw.com/) or [diagrams.net](http://diagrams.net). This is **highly** recommended!)*

1.  The **Rust Ingestion Service** accepts high-volume TCP connections and publishes raw location data to a Kafka topic.
2.  The **Go API Service** (as a consumer) reads from Kafka.
3.  For each message, it calculates the **H3 Geohash**.
4.  It performs a dual-write:
    * Saves the data to the **Redis Geospatial Index** (for the v2 API).
    * Saves the full, enriched data to the **PostgreSQL** database (for the v1 API and analytics).
5.  A rider's app can query the `/v2/find-drivers` endpoint to get an instant, cached response from Redis.

## ⚙️ Tech Stack

* **API & Consumer:** Go
* **Ingestion Service:** Rust (with Tokio)
* **Message Broker:** Kafka
* **Cache:** Redis (for `GeoRadius` queries)
* **Database:** PostgreSQL (with PostGIS for `ST_Distance` queries)
* **Containerization:** Docker & Docker Compose

## 🏁 How to Run

1.  Ensure you have Docker and Docker Compose installed.
2.  Clone the repository.
3.  From the root folder, start the entire backend stack:
    ```bash
    docker-compose up -d
    ```
4.  (Coming Soon: After we update the `docker-compose.yml` to use our new Dockerfiles, this will be the *only* command needed!)

---

*(Self-note: I still need to update the `docker-compose.yml` to build my new Dockerfiles directly!)*