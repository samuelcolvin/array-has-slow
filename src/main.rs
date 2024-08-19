use ahash::AHashSet;
use datafusion::arrow::array::{
    Array, BooleanArray, LargeStringArray, LargeStringBuilder, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::common::{exec_err, Result as DataFusionResult, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use rand::{thread_rng, Rng};
use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    run().await.unwrap();
}

async fn run() -> DataFusionResult<()> {
    let mut ctx = SessionContext::new();
    datafusion_functions_json::register_all(&mut ctx)?;
    const HAYSTACK_SIZE: usize = 20;

    let udf = ScalarUDF::from(InSet::new());
    ctx.register_udf(udf);

    let batch = create_batch()?;
    ctx.register_batch(&"test", batch)?;

    let first_x_names = COMMON_NAMES
        .iter()
        .take(HAYSTACK_SIZE)
        .map(|s| format!("'{s}'"))
        .collect::<Vec<_>>()
        .join(", ");

    run_sql(
        &ctx,
        &format!("SELECT count(*) FROM test where trace_id in ({first_x_names})"),
    )
    .await?;

    let ors = COMMON_NAMES
        .iter()
        .take(HAYSTACK_SIZE)
        .map(|s| format!("trace_id='{s}'"))
        .collect::<Vec<_>>()
        .join(" OR ");

    run_sql(&ctx, &format!("SELECT count(*) FROM test where {ors}")).await?;

    run_sql(
        &ctx,
        &format!("SELECT count(*) FROM test where in_set(trace_id, [{first_x_names}])"),
    )
    .await?;
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

#[derive(Debug)]
struct InSet {
    signature: Signature,
}

impl InSet {
    fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::LargeUtf8,
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for InSet {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "in_set"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
        let Some(ColumnarValue::Array(needles)) = args.first() else {
            return exec_err!("in_set expects 2 argument, got no arguments");
        };
        let Some(needles) = needles.as_any().downcast_ref::<LargeStringArray>() else {
            return exec_err!("in_set expects 2 string argument, got a non string array");
        };

        let Some(ColumnarValue::Scalar(ScalarValue::List(list))) = args.get(1) else {
            return exec_err!("in_set expects 2 argument, got one argument");
        };
        let items = list.iter().next().unwrap().unwrap();
        let items = items.as_any().downcast_ref::<StringArray>().unwrap();
        let haystack = items
            .iter()
            .filter_map(|v| v.map(|s| s.to_string()))
            .collect::<AHashSet<_>>();
        let a = BooleanArray::from_unary(needles, |needle| haystack.contains(needle));
        Ok(ColumnarValue::Array(Arc::new(a)))
    }
}

const ROWS: usize = 10_000;

fn create_batch() -> Result<RecordBatch, ArrowError> {
    let mut rng = thread_rng();

    let mut str_builder = LargeStringBuilder::with_capacity(ROWS, ROWS * 100);

    for _ in 0..ROWS {
        let name = COMMON_NAMES[rng.gen_range(0..COMMON_NAMES.len())];
        str_builder.append_value(name);
    }

    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::LargeUtf8,
            true,
        )])),
        vec![Arc::new(str_builder.finish())],
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
