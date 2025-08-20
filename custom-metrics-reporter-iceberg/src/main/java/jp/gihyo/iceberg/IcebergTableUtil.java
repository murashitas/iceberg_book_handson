package jp.gihyo.iceberg;

import java.util.Map;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;


public class IcebergTableUtil {
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

}
