package jp.gihyo.iceberg;

import java.util.List;
import java.util.Map;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.function.Function;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.CommitMetricsResult;
import org.apache.iceberg.metrics.CounterResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IcebergTableUtil {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergTableUtil.class);
    private IcebergTableUtil() {}

    public static Table createOrLoadTable(Catalog catalog, TableIdentifier tableIdentifier, Schema schema) {
        if (catalog.tableExists(tableIdentifier)) {
            return catalog.loadTable(tableIdentifier);
        } else {
            Map<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
            return catalog.createTable(
                    tableIdentifier,
                    schema,
                    null,
                    properties
            );
        }
    }

    public static Schema getScanReportSchema() {
        return new Schema(
                Types.NestedField.required(1, "timestamp", Types.TimestampType.withZone()),
                Types.NestedField.required(2, "table_name", Types.StringType.get()),
                Types.NestedField.optional(3, "snapshot_id", Types.LongType.get()),
                Types.NestedField.optional(4, "filter", Types.StringType.get()),
                Types.NestedField.optional(5, "schema_id", Types.IntegerType.get()),
                Types.NestedField.optional(6, "projected_field_ids", Types.ListType.ofRequired(7, Types.IntegerType.get())),
                Types.NestedField.optional(8, "projected_field_names", Types.ListType.ofRequired(9, Types.StringType.get())),

                // Scan metrics
                Types.NestedField.optional(10, "total_planning_duration_ms", Types.LongType.get()),
                Types.NestedField.optional(11, "result_data_files", Types.LongType.get()),
                Types.NestedField.optional(12, "result_delete_files", Types.LongType.get()),
                Types.NestedField.optional(13, "scanned_data_manifests", Types.LongType.get()),
                Types.NestedField.optional(14, "skipped_data_manifests", Types.LongType.get()),
                Types.NestedField.optional(15, "total_data_manifests", Types.LongType.get()),
                Types.NestedField.optional(16, "total_delete_manifests", Types.LongType.get()),
                Types.NestedField.optional(17, "total_file_size_bytes", Types.LongType.get()),
                Types.NestedField.optional(18, "total_delete_file_size_bytes", Types.LongType.get()),
                Types.NestedField.optional(19, "skipped_data_files", Types.LongType.get()),
                Types.NestedField.optional(20, "skipped_delete_files", Types.LongType.get()),

                // Metadata
                Types.NestedField.optional(21, "metadata", Types.MapType.ofRequired(22, 23,
                        Types.StringType.get(), Types.StringType.get()))
        );
    }

    public static Schema getCommitReportSchema() {
        return new Schema(
                Types.NestedField.required(1, "timestamp", Types.TimestampType.withZone()),
                Types.NestedField.required(2, "table_name", Types.StringType.get()),
                Types.NestedField.optional(3, "snapshot_id", Types.LongType.get()),
                Types.NestedField.optional(4, "sequence_number", Types.LongType.get()),
                Types.NestedField.optional(5, "operation", Types.StringType.get()),

                // Commit metrics
                Types.NestedField.optional(6, "total_duration_ms", Types.LongType.get()),
                Types.NestedField.optional(7, "attempts", Types.LongType.get()),
                Types.NestedField.optional(8, "added_data_files", Types.LongType.get()),
                Types.NestedField.optional(9, "removed_data_files", Types.LongType.get()),
                Types.NestedField.optional(10, "total_data_files", Types.LongType.get()),
                Types.NestedField.optional(11, "added_delete_files", Types.LongType.get()),
                Types.NestedField.optional(12, "removed_delete_files", Types.LongType.get()),
                Types.NestedField.optional(13, "total_delete_files", Types.LongType.get()),
                Types.NestedField.optional(14, "added_records", Types.LongType.get()),
                Types.NestedField.optional(15, "removed_records", Types.LongType.get()),
                Types.NestedField.optional(16, "total_records", Types.LongType.get()),
                Types.NestedField.optional(17, "added_files_size_bytes", Types.LongType.get()),
                Types.NestedField.optional(18, "removed_files_size_bytes", Types.LongType.get()),
                Types.NestedField.optional(19, "total_files_size_bytes", Types.LongType.get()),

                // Metadata
                Types.NestedField.optional(20, "metadata", Types.MapType.ofRequired(21, 22,
                        Types.StringType.get(), Types.StringType.get()))
        );
    }

    private static <T extends MetricsReport> void writeMetricsReports(
            Table metricsReportTable,
            List<T> reports,
            Function<T, Record> metricsReportToRecord) throws NoSuchTableException {
        
        // Try to get active SparkSession
        SparkSession spark = getActiveSparkSession();
        
        if (spark != null) {
            writeRecordsWithSpark(spark, metricsReportTable, reports, metricsReportToRecord);
        } else {
            throw new RuntimeException("No spark session available. Currently SparkSession is required to write metrics reports.");
        }
    }

    public static void writeScanReports(Table scanReportTable, List<ScanReport> reports) throws NoSuchTableException {
        writeMetricsReports(scanReportTable, reports, IcebergTableUtil::scanReportToRecord);
    }

    public static void writeCommitReports(Table commitReportTable, List<CommitReport> reports) throws NoSuchTableException {
        writeMetricsReports(commitReportTable, reports, IcebergTableUtil::commitReportToRecord);
    }

    private static Record scanReportToRecord(ScanReport report) {
        GenericRecord record = GenericRecord.create(getScanReportSchema());

        record.setField("timestamp", OffsetDateTime.now(ZoneOffset.UTC));
        record.setField("table_name", report.tableName());
        record.setField("snapshot_id", report.snapshotId());
        record.setField("filter", report.filter() != null ? report.filter().toString() : null);
        record.setField("schema_id", report.schemaId());
        record.setField("projected_field_ids", report.projectedFieldIds());
        record.setField("projected_field_names", report.projectedFieldNames());

        // Scan metrics
        ScanMetricsResult metrics = report.scanMetrics();
        if (metrics != null) {
            if (metrics.totalPlanningDuration() != null) {
                record.setField("total_planning_duration_ms",
                        metrics.totalPlanningDuration().totalDuration().toMillis());
            }
            setLongField(record, "result_data_files", metrics.resultDataFiles());
            setLongField(record, "result_delete_files", metrics.resultDeleteFiles());
            setLongField(record, "scanned_data_manifests", metrics.scannedDataManifests());
            setLongField(record, "skipped_data_manifests", metrics.skippedDataManifests());
            setLongField(record, "total_data_manifests", metrics.totalDataManifests());
            setLongField(record, "total_delete_manifests", metrics.totalDeleteManifests());
            setLongField(record, "total_file_size_bytes", metrics.totalFileSizeInBytes());
            setLongField(record, "total_delete_file_size_bytes", metrics.totalDeleteFileSizeInBytes());
            setLongField(record, "skipped_data_files", metrics.skippedDataFiles());
            setLongField(record, "skipped_delete_files", metrics.skippedDeleteFiles());
        }

        record.setField("metadata", report.metadata());
        return record;
    }

    private static Record commitReportToRecord(CommitReport report) {
        GenericRecord record = GenericRecord.create(getCommitReportSchema());

        record.setField("timestamp", OffsetDateTime.now(ZoneOffset.UTC));
        record.setField("table_name", report.tableName());
        record.setField("snapshot_id", report.snapshotId());
        record.setField("sequence_number", report.sequenceNumber());
        record.setField("operation", report.operation());

        // Commit metrics
        CommitMetricsResult metrics = report.commitMetrics();
        if (metrics != null) {
            if (metrics.totalDuration() != null) {
                record.setField("total_duration_ms",
                        metrics.totalDuration().totalDuration().toMillis());
            }
            setLongField(record, "attempts", metrics.attempts());
            setLongField(record, "added_data_files", metrics.addedDataFiles());
            setLongField(record, "removed_data_files", metrics.removedDataFiles());
            setLongField(record, "total_data_files", metrics.totalDataFiles());
            setLongField(record, "added_delete_files", metrics.addedDeleteFiles());
            setLongField(record, "removed_delete_files", metrics.removedDeleteFiles());
            setLongField(record, "total_delete_files", metrics.totalDeleteFiles());
            setLongField(record, "added_records", metrics.addedRecords());
            setLongField(record, "removed_records", metrics.removedRecords());
            setLongField(record, "total_records", metrics.totalRecords());
            setLongField(record, "added_files_size_bytes", metrics.addedFilesSizeInBytes());
            setLongField(record, "removed_files_size_bytes", metrics.removedFilesSizeInBytes());
            setLongField(record, "total_files_size_bytes", metrics.totalFilesSizeInBytes());
        }

        record.setField("metadata", report.metadata());
        LOG.warn(record.toString());
        return record;
    }

    private static void setLongField(GenericRecord record, String fieldName, CounterResult counter) {
        if (counter != null) {
            record.setField(fieldName, counter.value());
        }
    }

    private static SparkSession getActiveSparkSession() {
        try {
            LOG.info("Getting active spark session");
            return SparkSession.active();
        } catch (IllegalStateException e) {
            return null;
        }
    }
    
    private static <T extends MetricsReport> void writeRecordsWithSpark(
            SparkSession spark,
            Table table,
            List<T> reports,
            Function<T, Record> metricsReportToRecord) throws NoSuchTableException {
        
        // Convert reports to Records
        List<Record> records = reports.stream()
                .map(metricsReportToRecord)
                .toList();
        
        // Convert Records to Rows using Spark's built-in capabilities
        List<Row> sparkRows = records.stream()
                .map(record -> recordToSparkRow(record, table.schema()))
                .toList();

        Dataset<Row> df = spark.createDataFrame(sparkRows, convertToSparkSchema(table.schema()));

        // Write to Iceberg table using table location
        String currentCatalog = spark.catalog().currentCatalog();
        String metricsCatalogName = table.name().split("\\.")[0];
        spark.catalog().setCurrentCatalog(metricsCatalogName);
        df.writeTo(table.name()).append();
        spark.catalog().setCurrentCatalog(currentCatalog);
    }
    
    private static Row recordToSparkRow(Record record, Schema schema) {
        Object[] values = new Object[schema.columns().size()];
        int i = 0;
        for (Types.NestedField field : schema.columns()) {
            Object value = record.getField(field.name());
            // Convert OffsetDateTime to java.sql.Timestamp for Spark compatibility
            if (value instanceof OffsetDateTime offsetDateTime) {
                value = java.sql.Timestamp.from(offsetDateTime.toInstant());
            }
            values[i++] = value;
        }
        return RowFactory.create(values);
    }
    
    private static StructType convertToSparkSchema(Schema icebergSchema) {
        List<StructField> fields = new java.util.ArrayList<>();
        
        for (Types.NestedField field : icebergSchema.columns()) {
            DataType sparkType = convertToSparkType(field.type());
            boolean nullable = !field.isRequired();
            fields.add(DataTypes.createStructField(field.name(), sparkType, nullable));
        }
        
        return DataTypes.createStructType(fields);
    }
    
    private static DataType convertToSparkType(org.apache.iceberg.types.Type type) {
        return switch (type.typeId()) {
            case BOOLEAN -> DataTypes.BooleanType;
            case INTEGER -> DataTypes.IntegerType;
            case LONG -> DataTypes.LongType;
            case FLOAT -> DataTypes.FloatType;
            case DOUBLE -> DataTypes.DoubleType;
            case DATE -> DataTypes.DateType;
            case TIMESTAMP -> DataTypes.TimestampType;
            case STRING -> DataTypes.StringType;
            case BINARY -> DataTypes.BinaryType;
            case LIST -> {
                Types.ListType listType = (Types.ListType) type;
                yield DataTypes.createArrayType(
                        convertToSparkType(listType.elementType()),
                        !listType.isElementRequired()
                );
            }
            case MAP -> {
                Types.MapType mapType = (Types.MapType) type;
                yield DataTypes.createMapType(
                        convertToSparkType(mapType.keyType()),
                        convertToSparkType(mapType.valueType()),
                        !mapType.isValueRequired()
                );
            }
            default ->
                // For complex types, default to StringType
                    DataTypes.StringType;
        };
    }

}
