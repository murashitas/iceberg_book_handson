package jp.gihyo.iceberg;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;

import java.util.Map;

public class IcebergTableMetricsReporter implements MetricsReporter {
    private static final String CATALOG_NAME = "catalog-name";
    private static final String CATALOG_IMPL = "catalog-impl";
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

        // TODO: Add the implementation of write metrics to each table
        System.out.println(report);

    }

    private void initializeCatalog() {
        String catalogName = properties.get(CATALOG_NAME);
        String catalogImpl = properties.get(CATALOG_IMPL);
        this.catalog = CatalogUtil.loadCatalog(catalogImpl, catalogName, properties, null);
    }

    private void createOrLoadMetricsTable(Catalog catalog, String ns, String metricsTableName) {
        Schema scanReportTableSchema = IcebergTableUtil.getCommitReportSchema();
        Schema commitReportTableSchema = IcebergTableUtil.getCommitReportSchema();

        TableIdentifier scanReportTableId = TableIdentifier.of(ns, metricsTableName + "_scan");
        TableIdentifier commitReportTableId = TableIdentifier.of(ns,  metricsTableName + "_commit");

        this.scanReportTable = IcebergTableUtil.createOrLoadTable(catalog, scanReportTableId, scanReportTableSchema);
        this.commitReportTable = IcebergTableUtil.createOrLoadTable(catalog, commitReportTableId, commitReportTableSchema);
    }
}