//! Benchmark command for Wings HTTP ingestor.
//!
//! This module provides benchmarking capabilities for the Wings HTTP ingestor,
//! allowing users to test performance and throughput under various conditions.

use std::fs;
use std::sync::Arc;
use std::time::Duration;

use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_schema::Field;
use clap::Args;
use error_stack::ResultExt;
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use wings_ingestor_http::types::{Batch, PushRequest};
use wings_metadata_core::admin::{Admin, NamespaceName, TenantName, TopicName, TopicOptions};
use wings_metadata_core::partition::PartitionValue;

use crate::error::{CliError, CliResult};
use crate::remote::RemoteArgs;

/// Arguments for the benchmark command.
///
/// This command benchmarks the Wings HTTP ingestor by continuously pushing
/// JSON data to the specified topic until interrupted with Ctrl+C.
///
/// # Example Usage
///
/// Basic benchmark with default settings:
/// ```bash
/// wings bench '{"user_id": 123, "event": "login"}'
/// ```
///
/// Advanced benchmark with custom settings:
/// ```bash
/// wings bench '{"temperature": 25.5, "humidity": 60}' \
///   --batch-size 5000 \
///   --concurrency 10 \
///   --partition-value '{"String": "sensor-1"}' \
///   --namespace production \
///   --topic sensor-data
/// ```
#[derive(Debug, Args)]
pub struct BenchArgs {
    /// JSON data to push to the topic, or path to file containing JSON data.
    ///
    /// This should be a valid JSON object that represents the structure
    /// of the data you want to benchmark. The schema will be automatically
    /// inferred from this JSON and used to create the topic.
    ///
    /// If the argument starts with '@', it will be treated as a file path
    /// containing one JSON object per line. The benchmark will cycle through
    /// these objects when creating batches.
    ///
    /// Example: '{"user_id": 123, "event": "login", "timestamp": "2024-01-01T00:00:00Z"}'
    /// Example: '@data.jsonl' (file with one JSON object per line)
    json_data: String,

    /// HTTP ingestor address.
    ///
    /// The address where the Wings HTTP ingestor is running. Should match
    /// the address used in the 'wings dev' command.
    #[arg(long, default_value = "http://127.0.0.1:7780")]
    http_address: String,

    /// Number of rows to include in a single batch.
    ///
    /// Each HTTP request will contain this many copies of the JSON data.
    /// Larger batch sizes can improve throughput but may increase latency.
    #[arg(long, default_value = "1000")]
    batch_size: usize,

    /// Partition value for the data (JSON format).
    ///
    /// If specified, all data will be sent to this partition. The value
    /// should be a JSON representation of a PartitionValue enum variant.
    ///
    /// Examples:
    /// - String partition: '{"String": "user-123"}'
    /// - Integer partition: '{"Int32": 42}'
    /// - Boolean partition: '{"Boolean": true}'
    /// - Null partition: '"Null"'
    #[arg(long)]
    partition_value: Option<String>,

    /// Number of concurrent tasks to run.
    ///
    /// Each task will continuously send batches of data to the HTTP ingestor.
    /// Higher concurrency can increase throughput but may overwhelm the server.
    #[arg(long, default_value = "1")]
    concurrency: usize,

    /// Namespace to use for the benchmark.
    ///
    /// The namespace must exist in the Wings admin service. In development
    /// mode, a "default" namespace is automatically created.
    #[arg(long, default_value = "default")]
    namespace: String,

    /// Tenant to use for the benchmark.
    ///
    /// The tenant must exist in the Wings admin service. In development
    /// mode, a "default" tenant is automatically created.
    #[arg(long, default_value = "default")]
    tenant: String,

    /// Topic to push data to.
    ///
    /// The topic will be automatically created if it doesn't exist, or
    /// recreated if it does exist. The schema will be inferred from the
    /// provided JSON data.
    #[arg(long, default_value = "bench")]
    topic: String,

    #[clap(flatten)]
    remote: RemoteArgs,
}

impl BenchArgs {
    pub async fn run(self, ct: CancellationToken) -> CliResult<()> {
        // Parse the JSON data or load from file
        let json_values = if self.json_data.starts_with('@') {
            let file_path = fs::canonicalize(&self.json_data[1..]).change_context(
                CliError::InvalidConfiguration {
                    message: format!("invalid file path: {}", &self.json_data[1..]),
                },
            )?;

            let file_content =
                fs::read_to_string(&file_path).change_context(CliError::InvalidConfiguration {
                    message: format!("failed to read file: {}", file_path.display()),
                })?;

            let mut values = Vec::new();
            for (line_num, line) in file_content.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let json_value: Value =
                    serde_json::from_str(line).change_context(CliError::InvalidConfiguration {
                        message: format!("failed to parse JSON on line {}: {}", line_num + 1, line),
                    })?;
                values.push(json_value);
            }

            if values.is_empty() {
                return Err(CliError::InvalidConfiguration {
                    message: "file contains no valid JSON data".to_string(),
                }
                .into());
            }

            values
        } else {
            let json_value: Value = serde_json::from_str(&self.json_data).change_context(
                CliError::InvalidConfiguration {
                    message: "failed to parse JSON data".to_string(),
                },
            )?;
            vec![json_value]
        };

        // Parse partition value if provided
        let partition_value = if let Some(ref partition_str) = self.partition_value {
            let partition_json: Value = serde_json::from_str(partition_str).change_context(
                CliError::InvalidConfiguration {
                    message: "failed to parse partition value JSON".to_string(),
                },
            )?;

            let partition_value: PartitionValue = serde_json::from_value(partition_json)
                .change_context(CliError::InvalidConfiguration {
                    message: "failed to deserialize partition value".to_string(),
                })?;

            Some(partition_value)
        } else {
            None
        };

        // Create admin client
        let admin_client = self.remote.admin_client().await?;

        // Create resource names
        let tenant_name =
            TenantName::new(&self.tenant).change_context(CliError::InvalidConfiguration {
                message: format!("invalid tenant name: {}", self.tenant),
            })?;

        let namespace_name = NamespaceName::new(&self.namespace, tenant_name).change_context(
            CliError::InvalidConfiguration {
                message: format!("invalid namespace name: {}", self.namespace),
            },
        )?;

        let topic_name = TopicName::new(&self.topic, namespace_name.clone()).change_context(
            CliError::InvalidConfiguration {
                message: format!("invalid topic name: {}", self.topic),
            },
        )?;

        // Ensure topic exists (use first JSON value for schema inference)
        self.ensure_topic_exists(&admin_client, &topic_name, &json_values)
            .await?;

        // Create HTTP client
        let http_client = reqwest::Client::new();

        // Create benchmark data
        let benchmark_data = BenchmarkData::new(
            self.batch_size,
            json_values,
            partition_value,
            namespace_name.clone(),
            topic_name.clone(),
        );

        println!(
            "Starting benchmark with {} concurrent tasks",
            self.concurrency
        );
        println!("Pushing to namespace: {}", namespace_name);
        println!("Topic: {}", topic_name);
        println!("Batch size: {}", self.batch_size);
        println!("Partition value: {:?}", benchmark_data.partition_value);
        println!("HTTP address: {}", self.http_address);
        println!("Press Ctrl+C to stop");

        // Statistics tracking
        let stats = Arc::new(BenchStats::new());

        // Spawn benchmark tasks
        let mut handles = Vec::new();
        for task_id in 0..self.concurrency {
            let ct = ct.clone();
            let http_client = http_client.clone();
            let http_address = self.http_address.clone();
            let benchmark_data = benchmark_data.clone();
            let stats = stats.clone();
            let batch_size = self.batch_size;

            let handle = tokio::spawn(async move {
                bench_task(
                    task_id,
                    ct,
                    http_client,
                    http_address,
                    benchmark_data,
                    batch_size,
                    stats,
                )
                .await
            });

            handles.push(handle);
        }

        // Spawn stats reporting task
        let stats_handle = {
            let stats = stats.clone();
            let ct = ct.clone();

            tokio::spawn(async move { stats_reporter(ct, stats).await })
        };

        // Wait for all tasks to complete
        for handle in handles {
            let _ = handle.await;
        }

        let _ = stats_handle.await;

        // Print final statistics
        let final_stats = stats.get_stats();
        println!("\nFinal statistics:");
        println!("Total requests: {}", final_stats.total_requests);
        println!("Total errors: {}", final_stats.total_errors);
        println!("Total records: {}", final_stats.total_records);
        println!("Elapsed time: {:?}", final_stats.elapsed);

        Ok(())
    }

    async fn ensure_topic_exists(
        &self,
        admin_client: &impl Admin,
        topic_name: &TopicName,
        json_values: &[Value],
    ) -> CliResult<()> {
        // Try to get the topic
        match admin_client.get_topic(topic_name.clone()).await {
            Ok(_) => {
                println!("Topic {} exists, deleting it", topic_name);
                // Delete the existing topic
                admin_client
                    .delete_topic(topic_name.clone(), true)
                    .await
                    .change_context(CliError::AdminApi {
                        message: "failed to delete existing topic".to_string(),
                    })?;
            }
            Err(_) => {
                // Topic doesn't exist, which is fine
                println!("Topic {} does not exist", topic_name);
            }
        }

        // Infer schema from JSON
        let schema = infer_json_schema_from_iterator(json_values.iter().map(Result::Ok))
            .change_context(CliError::InvalidConfiguration {
                message: "failed to infer schema from JSON".to_string(),
            })?;

        // Convert Arrow schema to topic fields
        let fields: Vec<Field> = schema.fields().iter().map(|f| (**f).clone()).collect();

        // Create topic options
        let topic_options = TopicOptions::new(fields);

        // Create the topic
        println!("Creating topic {} with schema", topic_name);
        admin_client
            .create_topic(topic_name.clone(), topic_options)
            .await
            .change_context(CliError::AdminApi {
                message: "failed to create topic".to_string(),
            })?;

        println!("Topic {} created successfully", topic_name);
        Ok(())
    }
}

/// Individual benchmark task that continuously sends HTTP requests.
///
/// This function runs in a loop, creating batches of data and sending them
/// to the HTTP ingestor until the cancellation token is triggered.
async fn bench_task(
    task_id: usize,
    ct: CancellationToken,
    http_client: reqwest::Client,
    http_address: String,
    benchmark_data: BenchmarkData,
    batch_size: usize,
    stats: Arc<BenchStats>,
) {
    let push_url = format!("{}/v1/push", http_address);

    let (request, batch_size_bytes) = benchmark_data.create_push_request();

    while !ct.is_cancelled() {
        let start_time = std::time::Instant::now();
        let result = http_client.post(&push_url).json(&request).send().await;
        let elapsed = start_time.elapsed();

        match result {
            Ok(response) => {
                if response.status().is_success() {
                    stats.record_success(batch_size, batch_size_bytes, elapsed);
                } else {
                    stats.record_error();
                    eprintln!(
                        "Task {} HTTP error: {} - {}",
                        task_id,
                        response.status(),
                        response.text().await.unwrap_or_default()
                    );
                }
            }
            Err(e) => {
                stats.record_error();
                eprintln!("Task {} request error: {}", task_id, e);
            }
        }
    }
}

/// Background task that periodically reports benchmark statistics.
///
/// This function runs concurrently with the benchmark tasks and prints
/// progress updates every 5 seconds until cancellation.
async fn stats_reporter(ct: CancellationToken, stats: Arc<BenchStats>) {
    let mut interval = tokio::time::interval(Duration::from_secs(5));

    while !ct.is_cancelled() {
        tokio::select! {
            _ = interval.tick() => {
                let current_stats = stats.get_stats();

                let records_sec = (current_stats.total_records as f64) / current_stats.elapsed.as_secs_f64();
                let total_bytes = bytesize::ByteSize(current_stats.total_bytes);
                let bytes_sec = (current_stats.total_bytes as f64) / current_stats.elapsed.as_secs_f64();
                let bytes_sec = bytesize::ByteSize(bytes_sec as u64);

                println!(
                    "Stats: {} req, {} err, {} msg ({:.2} msg/sec), {} ({}/sec), avg latency: {:.2}ms",
                    current_stats.total_requests,
                    current_stats.total_errors,
                    current_stats.total_records,
                    records_sec,
                    total_bytes,
                    bytes_sec,
                    current_stats.avg_latency_ms
                );
            }
            _ = ct.cancelled() => break,
        }
    }
}

#[derive(Debug, Clone)]
struct BenchStatsSnapshot {
    elapsed: std::time::Duration,
    total_requests: u64,
    total_errors: u64,
    total_records: u64,
    total_bytes: u64,
    avg_latency_ms: f64,
}

/// Thread-safe statistics collector for benchmark metrics.
///
/// Uses atomic operations to safely collect statistics from multiple
/// concurrent benchmark tasks.
struct BenchStats {
    start: std::time::Instant,
    total_requests: std::sync::atomic::AtomicU64,
    total_errors: std::sync::atomic::AtomicU64,
    total_records: std::sync::atomic::AtomicU64,
    total_bytes: std::sync::atomic::AtomicU64,
    total_latency_ms: std::sync::atomic::AtomicU64,
}

impl BenchStats {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
            total_requests: std::sync::atomic::AtomicU64::new(0),
            total_errors: std::sync::atomic::AtomicU64::new(0),
            total_records: std::sync::atomic::AtomicU64::new(0),
            total_bytes: std::sync::atomic::AtomicU64::new(0),
            total_latency_ms: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn record_success(&self, records: usize, size_bytes: usize, latency: Duration) {
        self.total_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.total_records
            .fetch_add(records as u64, std::sync::atomic::Ordering::Relaxed);
        self.total_bytes
            .fetch_add(size_bytes as u64, std::sync::atomic::Ordering::Relaxed);
        self.total_latency_ms.fetch_add(
            latency.as_millis() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn record_error(&self) {
        self.total_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.total_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn get_stats(&self) -> BenchStatsSnapshot {
        let total_requests = self
            .total_requests
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_errors = self.total_errors.load(std::sync::atomic::Ordering::Relaxed);
        let total_records = self
            .total_records
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_bytes = self.total_bytes.load(std::sync::atomic::Ordering::Relaxed);
        let total_latency_ms = self
            .total_latency_ms
            .load(std::sync::atomic::Ordering::Relaxed);

        let avg_latency_ms = if total_requests > 0 {
            total_latency_ms as f64 / total_requests as f64
        } else {
            0.0
        };

        let elapsed = self.start.elapsed();

        BenchStatsSnapshot {
            elapsed,
            total_requests,
            total_errors,
            total_records,
            total_bytes,
            avg_latency_ms,
        }
    }
}

/// Data structure containing benchmark configuration and methods for creating batches.
#[derive(Debug, Clone)]
struct BenchmarkData {
    batch_size: usize,
    json_values: Vec<Value>,
    partition_value: Option<PartitionValue>,
    namespace_name: NamespaceName,
    topic_name: TopicName,
}

impl BenchmarkData {
    /// Create a new benchmark data instance.
    fn new(
        batch_size: usize,
        json_values: Vec<Value>,
        partition_value: Option<PartitionValue>,
        namespace_name: NamespaceName,
        topic_name: TopicName,
    ) -> Self {
        Self {
            batch_size,
            json_values,
            partition_value,
            namespace_name,
            topic_name,
        }
    }

    /// Create a new batch with the specified number of records.
    fn create_batch(&self) -> (Batch, usize) {
        let mut data = Vec::with_capacity(self.batch_size);

        let mut size = 0;
        for i in 0..self.batch_size {
            let json_index = i % self.json_values.len();
            let json_value = self.json_values[json_index].clone();
            size += serde_json::to_string(&json_value).unwrap_or_default().len();
            data.push(json_value);
        }

        let batch = Batch {
            topic: self.topic_name.id().to_string(),
            partition: self.partition_value.clone(),
            data,
        };
        (batch, size)
    }

    /// Create a push request with the given batches.
    fn create_push_request(&self) -> (PushRequest, usize) {
        let (batch, data_size) = self.create_batch();
        let request = PushRequest {
            namespace: self.namespace_name.id().to_string(),
            batches: vec![batch],
        };
        (request, data_size)
    }
}
