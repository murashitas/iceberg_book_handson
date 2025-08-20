package jp.gihyo.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.Map;

public class IcebergTableMetricsReporter implements MetricsReporter {
    private static final IcebergTableMetricsReporter INSTANCE = new IcebergTableMetricsReporter();

    public static IcebergTableMetricsReporter getInstance() {
        return INSTANCE;
    }

    @Override
    public void initialize(Map<String, String> properties) {
        Catalog catalog = new RESTCatalog();
        catalog.initialize("rest_catalog", properties);

        String metricsNamespace = properties.get("namespace");

        createOrLoadMetricsTable(catalog, metricsNamespace);

    }

    @Override
    public void report(MetricsReport report) {
        System.out.println(report);
    }

    private void createOrLoadMetricsTable(Catalog catalog, String ns) {
        Schema scanReportTableSchema = IcebergTableUtil.getCommitReportSchema();
        Schema commitReportTableSchema = IcebergTableUtil.getCommitReportSchema();

        TableIdentifier scanReportTableId = TableIdentifier.of(ns, "report_scan");
        TableIdentifier commitReportTableId = TableIdentifier.of(ns,  "report_commit");

        IcebergTableUtil.createOrLoadTable(catalog, scanReportTableId, scanReportTableSchema);
        IcebergTableUtil.createOrLoadTable(catalog, commitReportTableId, commitReportTableSchema);
    }

}