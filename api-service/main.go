package main

import (
	"context" // We need this for the Redis library
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	// We need this for the Kafka consumer
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/go-redis/redis/v8" // Import the Redis library
	_ "github.com/lib/pq"
	"github.com/uber/h3-go/v3" // Import the H3 library
)

// DriverLocation struct (same as before)
type DriverLocation struct {
	DriverID  string  `json:"driver_id"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// --- THIS IS NEW ---
// This struct will represent the data we send
// *back* to the user. It's good practice to
// have separate structs for what you receive
// and what you send.
type DriverResponse struct {
	DriverID       string  `json:"driver_id"`
	DistanceMeters float64 `json:"distance_meters"`
}

// --- END OF NEW PART ---

var rdb *redis.Client

// We also need this 'context' variable for Redis.
// 'context.Background()' is a standard, empty context.
var ctx = context.Background()

const (
	// ... (const block is the same as before)
	host     = "localhost"
	port     = 5432
	user     = "user"
	password = "password"
	dbname   = "geodispatch"
)

// createDriversTable (same as before)
func createDriversTable(db *sql.DB) error {
	// We've added a 'geohash' column
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

//

// healthCheckHandler (same as before)
func healthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// ... (function is the same as before)
	log.Println("Received request for /health")
	fmt.Fprintf(w, "API service is up and running!")
}

// saveLocationToDB (same as before)
func saveLocationToDB(db *sql.DB, loc DriverLocation, geohash string) error {
	// The UPSERT query is updated to include the 4th value ($4)
	// and set the 'geohash' column.
	upsertSQL := `
	INSERT INTO drivers (driver_id, status, location, geohash)
	VALUES ($1, $2, ST_SetSRID(ST_Point($3, $4), 4326), $5)
	ON CONFLICT (driver_id) DO UPDATE SET
		status = $2,
		location = ST_SetSRID(ST_Point($3, $4), 4326),
		geohash = $5;
	`
	// Note: PostGIS is (lon, lat)
	_, err := db.Exec(upsertSQL, loc.DriverID, "online", loc.Longitude, loc.Latitude, geohash)

	if err != nil {
		return fmt.Errorf("failed to upsert driver location: %w", err)
	}
	return nil
}

func saveLocationToRedis(loc DriverLocation, geohash string) error {
	// Task 1: Save to the Geospatial Index (for our v2 API)
	// This is the same as before.
	err := rdb.GeoAdd(ctx, "driver_locations", &redis.GeoLocation{
		Name:      loc.DriverID,
		Longitude: loc.Longitude,
		Latitude:  loc.Latitude,
	}).Err()
	if err != nil {
		return fmt.Errorf("failed to save to GEO index: %w", err)
	}

	// Task 2: Save the *full* data to a Redis Hash
	// This is a more complete way to store our data.
	driverKey := fmt.Sprintf("driver:%s", loc.DriverID)

	// 'HSet' (Hash Set) sets fields in a hash (like a map)
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

// startKafkaConsumer (same as before)
func startKafkaConsumer(db *sql.DB) {
	// ... (consumer creation is the same)
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "geo-dispatch-api",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Printf("Failed to create Kafka consumer: %v\n", err)
		os.Exit(1)
	}

	c.SubscribeTopics([]string{"driver-locations"}, nil)
	log.Println("Kafka consumer loop started. Waiting for messages...")

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			log.Printf("KAFKA MESSAGE on %s: %s\n", msg.TopicPartition, string(msg.Value))

			// 1. Parse JSON (same as before)
			var loc DriverLocation
			err := json.Unmarshal(msg.Value, &loc)
			if err != nil {
				log.Printf("Failed to parse JSON: %v\n", err)
				continue
			}

			// --- THIS IS THE NEW LOGIC ---
			// 2. Calculate H3 Geohash
			// We use H3 resolution 9 (a very small grid,
			// about 175m wide).
			h3Index := h3.FromGeo(h3.GeoCoord{Latitude: loc.Latitude, Longitude: loc.Longitude}, 9)
			geohash := h3.ToString(h3Index)
			// --- END OF NEW LOGIC ---

			// 3. Save to Postgres (pass the new geohash)
			err = saveLocationToDB(db, loc, geohash)
			if err != nil {
				log.Printf("Failed to save to DB: %v\n", err)
			} else {
				log.Printf("Successfully saved to POSTGRES for driver %s (geohash: %s)\n", loc.DriverID, geohash)
			}

			// 4. Save to Redis (pass the new geohash)
			err = saveLocationToRedis(loc, geohash)
			if err != nil {
				log.Printf("Failed to save to REDIS: %v\n", err)
			} else {
				log.Printf("Successfully saved to REDIS for driver %s\n", loc.DriverID)
			}

		} else {
			log.Printf("Kafka consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}

// --- THIS IS THE NEW API HANDLER ---
// This function will handle requests to /find-drivers
// It needs the 'db' connection, so we "wrap" it
// in another function.
func findDriversHandler(db *sql.DB) http.HandlerFunc {
	// This is the actual function that handles the request
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Get the 'lat' and 'lon' from the URL
		// (e.g., /find-drivers?lat=40.71&lon=-74.00)
		latStr := r.URL.Query().Get("lat")
		lonStr := r.URL.Query().Get("lon")
		if latStr == "" || lonStr == "" {
			http.Error(w, "Missing lat or lon query parameters", http.StatusBadRequest)
			return
		}

		// 2. Convert the string parameters to numbers (float64)
		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			http.Error(w, "Invalid lat parameter", http.StatusBadRequest)
			return
		}
		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			http.Error(w, "Invalid lon parameter", http.StatusBadRequest)
			return
		}

		// 3. Run the "Nearest Neighbor" SQL query!
		// This is the core logic.
		querySQL := `
		SELECT 
			driver_id,
			ST_Distance(
				location, 
				ST_SetSRID(ST_Point($1, $2), 4326)
			) as distance_meters
		FROM drivers
		WHERE status = 'online'
		ORDER BY location <-> ST_SetSRID(ST_Point($1, $2), 4326)
		LIMIT 5;
		`
		// 'ST_Distance' calculates the distance in meters
		// '<->' is the special PostGIS "nearest neighbor" operator
		// It's *very* fast because it uses the R-tree index.

		// 4. Execute the query
		// We use 'db.Query' (not 'Exec') because we expect rows back.
		// We pass 'lon' then 'lat' because PostGIS is (long, lat)
		rows, err := db.Query(querySQL, lon, lat)
		if err != nil {
			log.Printf("Failed to query DB: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
		defer rows.Close() // Make sure to close 'rows'

		// 5. Scan the results into our new struct
		var drivers []DriverResponse
		for rows.Next() { // Loop through each row
			var dr DriverResponse
			// 'Scan' copies the values from the row columns
			// into the fields of our 'dr' struct.
			if err := rows.Scan(&dr.DriverID, &dr.DistanceMeters); err != nil {
				log.Printf("Failed to scan row: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
				return
			}
			drivers = append(drivers, dr) // Add it to our list
		}

		// 6. Send the list back as JSON
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drivers)
	}
}

// --- END OF NEW PART ---

func findDriversRedisHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. Get query params (same as v1)
		latStr := r.URL.Query().Get("lat")
		lonStr := r.URL.Query().Get("lon")
		if latStr == "" || lonStr == "" {
			http.Error(w, "Missing lat or lon query parameters", http.StatusBadRequest)
			return
		}

		lat, err := strconv.ParseFloat(latStr, 64)
		if err != nil {
			http.Error(w, "Invalid lat parameter", http.StatusBadRequest)
			return
		}
		lon, err := strconv.ParseFloat(lonStr, 64)
		if err != nil {
			http.Error(w, "Invalid lon parameter", http.StatusBadRequest)
			return
		}

		// 2. Run the Redis Geospatial Query!
		// This is the core logic.
		// 'GeoRadius' finds all members in a radius
		// around a given lat/lon.
		locations, err := rdb.GeoRadius(ctx, "driver_locations", lon, lat, &redis.GeoRadiusQuery{
			Radius:   5000,  // Find drivers within 5000 meters
			Unit:     "m",   // Use 'm' for meters
			WithDist: true,  // Also return the distance
			Sort:     "ASC", // Sort by distance (Ascending)
			Count:    5,     // Only return the top 5
		}).Result()

		if err != nil {
			log.Printf("Failed to query Redis: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// 3. Scan the results into our response struct
		// The 'locations' slice is a list of 'redis.GeoLocation'
		drivers := make([]DriverResponse, len(locations))
		for i, loc := range locations {
			drivers[i] = DriverResponse{
				DriverID:       loc.Name,
				DistanceMeters: loc.Dist,
			}
		}

		// 4. Send the list back as JSON (same as v1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(drivers)
	}
}

func main() {
	// DB Connection (same as before)
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal("Failed to open DB connection:", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to ping DB:", err)
	}
	log.Println("Successfully connected to the database!")

	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // The address from docker-compose
		Password: "",               // No password set
		DB:       0,                // Use the default database
	})

	// Ping Redis to check the connection
	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	log.Println("Successfully connected to Redis!")

	// Create Table (same as before)
	err = createDriversTable(db)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Drivers table is ready.")

	// Start Kafka consumer (same as before)
	go startKafkaConsumer(db)

	// --- THIS IS UPDATED ---
	// Register our two handlers
	http.HandleFunc("/health", healthCheckHandler)
	http.HandleFunc("/find-drivers", findDriversHandler(db))

	// Add the new v2 handler
	http.HandleFunc("/v2/find-drivers", findDriversRedisHandler())

	// --- END OF UPDATED PART ---

	log.Println("Starting API service on http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
