use datafusion::arrow::array::{ListBuilder, RecordBatch, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::common::Result as DataFusionResult;
use datafusion::prelude::SessionContext;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    run().await.unwrap();
}

async fn run() -> DataFusionResult<()> {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx)?;

    let batch = create_batch()?;
    ctx.register_batch(&"test", batch)?;

    run_sql(
        &ctx,
        "SELECT count(*) FROM test where json_contains(json, 'service.name')",
    )
    .await?;
    run_sql(
        &ctx,
        "SELECT count(*) FROM test where array_has(list, 'service.name')",
    )
    .await?;
    // run_sql(&ctx, "SELECT count(*) FROM test where json ? 'service.name'")?;
    Ok(())
}

async fn run_sql(ctx: &SessionContext, sql: &str) -> DataFusionResult<()> {
    let start = Instant::now();
    let df = ctx.sql(&sql).await?;

    let batches = df.collect().await?;
    let elapsed = start.elapsed();
    print_batches(&batches)?;
    println!("mode: {sql}, query took {elapsed:?}");
    Ok(())
}

const ROWS: usize = 100_000;

fn create_batch() -> Result<RecordBatch, ArrowError> {
    let mut rng = thread_rng();

    let mut json_builder = StringBuilder::with_capacity(ROWS, ROWS * 100);

    let mut keys_builder =
        ListBuilder::with_capacity(StringBuilder::with_capacity(ROWS, ROWS * 10), ROWS * 10);

    for _ in 0..ROWS {
        let mut json = String::new();
        json.push_str("{");
        for i in 0..rng.gen_range(2..=18) {
            if json.len() > 1 {
                json.push_str(",");
            }
            json.push('"');
            let key = COMMON_NAMES[rng.gen_range(0..COMMON_NAMES.len())];
            keys_builder.values().append_value(key);

            json.push_str(key);
            json.push_str(r#"": "#);
            json.push_str(&i.to_string());
        }
        json.push(']');
        json_builder.append_value(&json);
        keys_builder.append(true);
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("json", DataType::Utf8, true),
            Field::new(
                "list",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ])),
        vec![
            Arc::new(json_builder.finish()),
            Arc::new(keys_builder.finish()),
        ],
    )
}

const COMMON_NAMES: [&str; 200] = [
    "service.name",
    "service.namespace",
    "service.version",
    "service.instance.id",
    "telemetry.sdk.name",
    "telemetry.sdk.language",
    "telemetry.sdk.version",
    "http.method",
    "http.url",
    "http.target",
    "http.host",
    "http.scheme",
    "http.status_code",
    "http.status_text",
    "http.flavor",
    "http.server_name",
    "http.client_ip",
    "http.user_agent",
    "http.request_content_length",
    "http.request_content_length_uncompressed",
    "http.response_content_length",
    "http.response_content_length_uncompressed",
    "http.route",
    "http.client_header",
    "http.server_header",
    "db.system",
    "db.connection_string",
    "db.user",
    "db.name",
    "db.statement",
    "db.operation",
    "db.instance",
    "db.url",
    "db.sql.table",
    "db.cassandra.keyspace",
    "db.cassandra.page_size",
    "db.cassandra.consistency_level",
    "db.cassandra.table",
    "db.cassandra.idempotence",
    "db.cassandra.speculative_execution_count",
    "db.cassandra.coordinator_id",
    "db.cassandra.coordinator_dc",
    "db.hbase.namespace",
    "db.redis.database_index",
    "db.mongodb.collection",
    "db.sql.dml",
    "db.sql.primary_key",
    "db.sql.foreign_key",
    "db.sql.index_name",
    "rpc.system",
    "rpc.service",
    "rpc.method",
    "rpc.grpc.status_code",
    "net.transport",
    "net.peer.ip",
    "net.peer.port",
    "net.peer.name",
    "net.peer.hostname",
    "net.peer.address_family",
    "net.peer.ip_version",
    "net.host.ip",
    "net.host.port",
    "net.host.name",
    "net.protocol.name",
    "net.protocol.version",
    "net.destination.ip",
    "net.destination.port",
    "net.destination.name",
    "net.destination.subnet",
    "net.host.connection.type",
    "net.host.connection.subtype",
    "net.host.captured.ip",
    "net.host.captured.port",
    "net.host.creator",
    "net.destination.dns",
    "net.source.captured.ip",
    "net.source.captured.port",
    "net.source.creator",
    "messaging.system",
    "messaging.destination",
    "messaging.destination_kind",
    "messaging.protocol",
    "messaging.protocol_version",
    "messaging.url",
    "messaging.message_id",
    "messaging.conversation_id",
    "messaging.payload_size",
    "messaging.payload_compressed_size",
    "exception.type",
    "exception.message",
    "exception.stacktrace",
    "exception.escaped",
    "event.name",
    "event.domain",
    "event.id",
    "event.timestamp",
    "event.dropped_attributes_count",
    "log.severity",
    "log.message",
    "log.record_id",
    "log.timestamp",
    "log.file.path",
    "log.file.line",
    "log.function",
    "metric.name",
    "metric.description",
    "metric.unit",
    "metric.value_type",
    "metric.aggregation",
    "span.id",
    "span.name",
    "span.kind",
    "span.start_time",
    "span.end_time",
    "span.status_code",
    "span.status_description",
    "span.dropped_attributes_count",
    "span.dropped_events_count",
    "span.dropped_links_count",
    "span.remote",
    "span.parent_span_id",
    "span.parent_trace_id",
    "tracer.name",
    "tracer.version",
    "trace.id",
    "trace.state",
    "host.id",
    "host.type",
    "host.image.name",
    "host.image.id",
    "host.image.version",
    "host.architecture",
    "host.os.type",
    "host.os.description",
    "host.os.version",
    "host.os.name",
    "host.process.id",
    "host.process.name",
    "host.process.command",
    "host.user.id",
    "host.user.name",
    "container.id",
    "container.name",
    "container.image.name",
    "container.image.tag",
    "k8s.pod.name",
    "k8s.pod.uid",
    "k8s.namespace.name",
    "k8s.node.name",
    "k8s.node.uid",
    "k8s.cluster.name",
    "k8s.container.name",
    "k8s.container.restart_count",
    "k8s.deployment.name",
    "k8s.statefulset.name",
    "k8s.daemonset.name",
    "k8s.job.name",
    "k8s.job.uid",
    "k8s.cronjob.name",
    "cloud.provider",
    "cloud.account.id",
    "cloud.region",
    "cloud.availability_zone",
    "cloud.platform",
    "cloud.service.name",
    "cloud.service.namespace",
    "cloud.service.instance.id",
    "cloud.instance.id",
    "cloud.instance.name",
    "cloud.machine.type",
    "faas.trigger",
    "faas.execution",
    "faas.id",
    "faas.name",
    "faas.version",
    "faas.instance",
    "faas.max_memory",
    "faas.execution_time",
    "faas.runtime",
    "faas.cold_start",
    "faas.timeout",
    "resource.type",
    "resource.attributes",
    "enduser.id",
    "enduser.role",
    "enduser.scope",
    "telemetry.source",
    "telemetry.destination",
    "telemetry.data_type",
    "telemetry.data_source",
    "telemetry.data_destination",
    "telemetry.data_state",
    "telemetry.data_id",
    "telemetry.action",
    "telemetry.resource",
    "telemetry.agent",
    "telemetry.version",
    "telemetry.status",
    "telemetry.config",
    "service.environment",
];
