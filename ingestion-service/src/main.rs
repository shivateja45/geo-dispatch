use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::spawn;

// This is an 'async' main function, enabled by tokio.
// It allows us to use 'await'.
#[tokio::main]
async fn main() {
    // 1. Create the Kafka Producer
    // We create it *once* and share it with all connections.
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");

    // 2. Start the TCP Listener
    let listener = TcpListener::bind("127.0.0.1:9000")
        .await
        .expect("Failed to bind TCP listener");

    println!("Ingestion service listening on port 9000...");

    // 3. Start the main "accept loop"
    // This loop waits for new connections.
    loop {
        // 'listener.accept()' waits for a new driver to connect.
        let (stream, addr) = listener.accept().await.unwrap();

        // A new driver connected!
        println!("New connection from: {}", addr);

        // Clone the producer. 'FutureProducer' is cheap to clone
        // (it's just a reference, like a pointer).
        let producer_clone = producer.clone();

        // 4. Spawn a new "task" (like a goroutine)
        // We use 'spawn' to handle this one driver in the
        // background, so we can go back to 'accept' more.
        spawn(async move {
            // Call our function to handle this specific driver
            handle_driver(stream, producer_clone).await;
            println!("Connection closed from: {}", addr);
        });
    }
}

// This function handles a single driver's connection
async fn handle_driver(
    stream: tokio::net::TcpStream,
    producer: FutureProducer,
) {
    // 'BufReader' helps us read data line-by-line
    let mut reader = BufReader::new(stream);
    let mut line_buf = String::new();
    let topic = "driver-locations";

    // This loop reads data from the driver
    loop {
        // 'read_line' waits for a new line of text
        // (e.g., {"driver_id": "789", ...})
        match reader.read_line(&mut line_buf).await {
            Ok(0) => break, // Connection closed by driver
            Ok(_) => {
                // We got a message!
                // 'line_buf' has the '\n' at the end, so we trim it.
                let message = line_buf.trim();
                println!("Received data: {}", message);

                // 5. Send the message to Kafka
                let record = FutureRecord::to(topic)
                    .payload(message)
                    .key("driver_key"); // A key helps Kafka organize data

                // 'send' is async, so we 'await' it
                match producer.send(record, Duration::from_secs(0)).await {
                    Ok(_) => println!("Sent message to Kafka"),
                    Err((e, _)) => println!("Error sending to Kafka: {}", e),
                }
            }
            Err(e) => {
                println!("Error reading from socket: {}", e);
                break;
            }
        }
        line_buf.clear(); // Clear the buffer for the next line
    }
}