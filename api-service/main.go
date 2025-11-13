package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/uber/h3-go/v3"
)

type DriverLocation struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

type DriverResponse struct {
	DriverID       string  `json:"driver_id"`
	DistanceMeters float64 `json:"distance_meters"`
}

var rdb *redis.Client
var ctx = context.Background()

const (
	host     = "localhost"
	port     = 5432
	user     = "user"
	password = "password"
	dbname   = "geodispatch"
)

func createDriversTable(db *sql.DB) error {
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS drivers (
		driver_id VARCHAR(255) PRIMARY KEY,
		status    VARCHAR(50),
		location  GEOGRAPHY(POINT, 4326),
		geohash   VARCHAR(15) 
	);
	`
	_, err := db.Exec(createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create drivers table: %w", err)
	}
	return nil
}

func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "API service is up and running!")
}

func saveLocationToDB(db *sql.DB, loc DriverLocation, geohash string) error {
	// Upsert: Insert new driver or update existing location/status
	upsertSQL := `
	INSERT INTO drivers (driver_id, status, location, geohash)
	VALUES ($1, $2, ST_SetSRID(ST_Point($3, $4), 4326), $5)
	ON CONFLICT (driver_id) DO UPDATE SET
		status = $2,
		location = ST_SetSRID(ST_Point($3, $4), 4326),
		geohash = $5;
	`
	// PostGIS uses (lon, lat) order
	_, err := db.Exec(upsertSQL, loc.DriverID, "online", loc.Longitude, loc.Latitude, geohash)
	if err != nil {
		return fmt.Errorf("failed to upsert driver location: %w", err)
	}
	return nil
}

func saveLocationToRedis(loc DriverLocation, geohash string) error {
	// 1. Update Geospatial Index for Radius Search
	err := rdb.GeoAdd(ctx, "driver_locations", &redis.GeoLocation{
		Name:      loc.DriverID,
		Longitude: loc.Longitude,
		Latitude:  loc.Latitude,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to save to GEO index: %w", err)
	}

	// 2. Store enriched driver data (Hash)
	driverKey := fmt.Sprintf("driver:%s", loc.DriverID)
	err = rdb.HSet(ctx, driverKey,
		"latitude", loc.Latitude,
		"longitude", loc.Longitude,
		"geohash", geohash,
	).Err()
	if err != nil {
		return fmt.Errorf("failed to save to HASH: %w", err)
	}
	return nil
}

func startKafkaConsumer(db *sql.DB) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "geo-dispatch-api",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer: %v\n", err)
	}

	c.SubscribeTopics([]string{"driver-locations"}, nil)
	log.Println("Kafka consumer started. Listening for updates...")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			var loc DriverLocation
			if err := json.Unmarshal(msg.Value, &loc); err != nil {
				log.Printf("Failed to parse JSON: %v\n", err)
				continue
			}

			// Calculate H3 Geohash (Resolution 9 ~174m edge length)
			h3Index := h3.FromGeo(h3.GeoCoord{Latitude: loc.Latitude, Longitude: loc.Longitude}, 9)
			geohash := h3.ToString(h3Index)

			// Dual-write to Postgres (Persistent) and Redis (Cache)
			if err := saveLocationToDB(db, loc, geohash); err != nil {
				log.Printf("Postgres Error: %v\n", err)
			}
			if err := saveLocationToRedis(loc, geohash); err != nil {
				log.Printf("Redis Error: %v\n", err)
			}

			// Logging only on success for high-throughput clarity
			// log.Printf("Processed driver %s\n", loc.DriverID)

		} else {
			log.Printf("Kafka consumer error: %v (%v)\n", err, msg)
		}
	}
}

// v1: PostGIS implementation using R-Tree index (KNN)
func findDriversHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		latStr := r.URL.Query().Get("lat")
		lonStr := r.URL.Query().Get("lon")

		lat, errLat := strconv.ParseFloat(latStr, 64)
		lon, errLon := strconv.ParseFloat(lonStr, 64)
		if errLat != nil || errLon != nil {
			http.Error(w, "Invalid coordinates", http.StatusBadRequest)
			return
		}

		querySQL := `
		SELECT 
			driver_id,
			ST_Distance(location, ST_SetSRID(ST_Point($1, $2), 4326)) as distance_meters
		FROM drivers
		WHERE status = 'online'
		ORDER BY location <-> ST_SetSRID(ST_Point($1, $2), 4326)
		LIMIT 5;
		`
		rows, err := db.Query(querySQL, lon, lat)
		if err != nil {
			log.Printf("DB Query Error: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var drivers []DriverResponse
		for rows.Next() {
			var dr DriverResponse
			if err := rows.Scan(&dr.DriverID, &dr.DistanceMeters); err != nil {
				continue
			}
			drivers = append(drivers, dr)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drivers)
	}
}

// v2: Redis implementation using In-Memory Geospatial Index
func findDriversRedisHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		latStr := r.URL.Query().Get("lat")
		lonStr := r.URL.Query().Get("lon")

		lat, errLat := strconv.ParseFloat(latStr, 64)
		lon, errLon := strconv.ParseFloat(lonStr, 64)
		if errLat != nil || errLon != nil {
			http.Error(w, "Invalid coordinates", http.StatusBadRequest)
			return
		}

		locations, err := rdb.GeoRadius(ctx, "driver_locations", lon, lat, &redis.GeoRadiusQuery{
			Radius:   5000, // 5km search radius
			Unit:     "m",
			WithDist: true,
			Sort:     "ASC",
			Count:    5,
		}).Result()

		if err != nil {
			log.Printf("Redis Query Error: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		drivers := make([]DriverResponse, len(locations))
		for i, loc := range locations {
			drivers[i] = DriverResponse{
				DriverID:       loc.Name,
				DistanceMeters: loc.Dist,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drivers)
	}
}

func main() {
	// 1. Initialize Postgres Connection
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Failed to open DB connection:", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatal("Failed to ping DB:", err)
	}
	log.Println("Connected to PostgreSQL")

	// 2. Initialize Redis Connection
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	log.Println("Connected to Redis")

	// 3. Run Migrations
	if err := createDriversTable(db); err != nil {
		log.Fatal(err)
	}

	// 4. Start Background Consumers
	go startKafkaConsumer(db)

	// 5. Start HTTP Server
	http.HandleFunc("/health", healthCheckHandler)
	http.HandleFunc("/find-drivers", findDriversHandler(db))       // v1: Postgres
	http.HandleFunc("/v2/find-drivers", findDriversRedisHandler()) // v2: Redis

	log.Println("Starting API service on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
