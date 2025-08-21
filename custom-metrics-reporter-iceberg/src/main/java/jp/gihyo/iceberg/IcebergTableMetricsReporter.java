package jp.gihyo.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.CommitReport;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IcebergTableMetricsReporter implements MetricsReporter {
    private static final String CATALOG_NAME = "catalog-name";
    private static final String CATALOG_NAME_DEFAULT = "metrics_catalog";
    private static final String NAMESPACE = "ns";
    private static final String NAMESPACE_DEFAULT = "db";
    private static final String TABLE_NAME_PREFIX = "table-prefix";
    private static final String TABLE_NAME_PREFIX_DEFAULT = "report";

    private Catalog catalog;
    private Map<String, String> properties;
    private Table scanReportTable;
    private Table commitReportTable;

    @Override
    public void initialize(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public void report(MetricsReport report) {
        initializeCatalog();

        String metricsNamespace = properties.getOrDefault(NAMESPACE, NAMESPACE_DEFAULT);
        createOrLoadMetricsTable(catalog, metricsNamespace, properties.getOrDefault(TABLE_NAME_PREFIX, TABLE_NAME_PREFIX_DEFAULT));

        List<MetricsReport> reports = new ArrayList<>();
        reports.add(report);
        if (report instanceof ScanReport) {
            try {
                IcebergTableUtil.writeScanReports(scanReportTable, reports.stream().map(record -> (ScanReport) record).toList());
            } catch (NoSuchTableException e) {
                throw new RuntimeException(e);
            }
        } else if (report instanceof CommitReport) {
            try {
                IcebergTableUtil.writeCommitReports(commitReportTable, reports.stream().map(record -> (CommitReport) record).toList());
            } catch (NoSuchTableException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void initializeCatalog() {
        String catalogName = properties.getOrDefault(CATALOG_NAME, CATALOG_NAME_DEFAULT);
        Catalog catalog = new RESTCatalog();
        catalog.initialize(catalogName, properties);
        this.catalog = catalog;
    }

    private void createOrLoadMetricsTable(Catalog catalog, String ns, String metricsTableName) {
        Schema scanReportTableSchema = IcebergTableUtil.getScanReportSchema();
        Schema commitReportTableSchema = IcebergTableUtil.getCommitReportSchema();

        TableIdentifier scanReportTableId = TableIdentifier.of(ns, metricsTableName + "_scan");
        TableIdentifier commitReportTableId = TableIdentifier.of(ns,  metricsTableName + "_commit");

        this.scanReportTable = IcebergTableUtil.createOrLoadTable(catalog, scanReportTableId, scanReportTableSchema);
        this.commitReportTable = IcebergTableUtil.createOrLoadTable(catalog, commitReportTableId, commitReportTableSchema);
    }
}