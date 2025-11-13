use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpListener;
use tokio::spawn;

#[tokio::main]
async fn main() {
    // Initialize Kafka Producer (shared across connections)
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Failed to create Kafka producer");

    let listener = TcpListener::bind("127.0.0.1:9000")
        .await
        .expect("Failed to bind TCP listener");

    println!("Ingestion service listening on port 9000...");

    loop {
        // Accept new driver connection
        let (stream, addr) = listener.accept().await.unwrap();
        // println!("New connection: {}", addr);

        let producer_clone = producer.clone();

        // Spawn lightweight task for this driver
        spawn(async move {
            handle_driver(stream, producer_clone).await;
        });
    }
}

async fn handle_driver(
    stream: tokio::net::TcpStream,
    producer: FutureProducer,
) {
    let mut reader = BufReader::new(stream);
    let mut line_buf = String::new();
    let topic = "driver-locations";

    loop {
        // Read line-delimited JSON from driver
        match reader.read_line(&mut line_buf).await {
            Ok(0) => break, // EOF
            Ok(_) => {
                let message = line_buf.trim();
                
                let record = FutureRecord::to(topic)
                    .payload(message)
                    .key("driver_key"); 

                // Fire and forget to Kafka
                if let Err((e, _)) = producer.send(record, Duration::from_secs(0)).await {
                    println!("Kafka Error: {}", e);
                }
            }
            Err(e) => {
                println!("Socket Error: {}", e);
                break;
            }
        }
        line_buf.clear();
    }
}