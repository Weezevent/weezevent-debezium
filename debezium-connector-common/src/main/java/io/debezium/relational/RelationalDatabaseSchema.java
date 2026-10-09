/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static io.debezium.openlineage.dataset.DatasetMetadata.DatasetKind.INPUT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumTaskState;
import io.debezium.openlineage.DebeziumOpenLineageEmitter;
import io.debezium.openlineage.dataset.DatasetMetadata;
import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Strings;

/**
 * A {@link DatabaseSchema} of a relational database such as Postgres. Provides information about the physical structure
 * of the database (the "database schema") as well as the structure of corresponding CDC messages (the "event schema").
 *
 * @author Gunnar Morling
 */
public abstract class RelationalDatabaseSchema implements DatabaseSchema<TableId> {
    private final static Logger LOG = LoggerFactory.getLogger(RelationalDatabaseSchema.class);

    private final RelationalDatabaseConnectorConfig config;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final TableSchemaBuilder schemaBuilder;
    private final TableFilter tableFilter;
    private final ColumnNameFilter columnFilter;
    private final ColumnMappers columnMappers;
    private final KeyMapper customKeysMapper;
    private final CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext;

    private final SchemasByTableId schemasByTableId;
    private final Tables tables;

    // Multi-tenant schema template (null when not configured) and the event schemas shared by structurally
    // identical tenant tables, keyed by SchemaTemplate#sharingKey.
    private final SchemaTemplate schemaSharingTemplate;
    private final Map<String, TableSchema> sharedTenantSchemas = new ConcurrentHashMap<>();

    protected RelationalDatabaseSchema(RelationalDatabaseConnectorConfig config, TopicNamingStrategy<TableId> topicNamingStrategy,
                                       TableFilter tableFilter, ColumnNameFilter columnFilter, TableSchemaBuilder schemaBuilder,
                                       boolean tableIdCaseInsensitive, KeyMapper customKeysMapper, CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext) {
        this.config = config;

        this.topicNamingStrategy = topicNamingStrategy;
        this.schemaBuilder = schemaBuilder;
        this.tableFilter = tableFilter;
        this.columnFilter = columnFilter;
        this.columnMappers = ColumnMappers.create(config);
        this.customKeysMapper = customKeysMapper;

        this.schemasByTableId = new SchemasByTableId(config.createSchemaStorage(tableIdCaseInsensitive));
        this.tables = new Tables(tableIdCaseInsensitive, config);
        this.taskContext = taskContext;
        this.schemaSharingTemplate = resolveSchemaSharingTemplate(config, topicNamingStrategy);
    }

    private static SchemaTemplate resolveSchemaSharingTemplate(RelationalDatabaseConnectorConfig config, TopicNamingStrategy<TableId> topicNamingStrategy) {
        final SchemaTemplate template = SchemaTemplate.from(config);
        if (template == null) {
            return null;
        }
        // Custom converters and key augmentation are resolved per table identifier inside the schema builder, so a
        // shared schema could carry another tenant's choice. Keep one schema per table in that case.
        final boolean customConverters = !Strings.isNullOrBlank(config.getConfig().getString(CommonConnectorConfig.CUSTOM_CONVERTERS));
        final boolean keyAugmented = topicNamingStrategy.keySchemaAugment() != TopicNamingStrategy.NO_SCHEMA_OP
                || topicNamingStrategy.keyValueAugment() != TopicNamingStrategy.NO_VALUE_OP;
        if (customConverters || keyAugmented) {
            LOG.warn("Schema template '{}' is configured but tenant event schemas will not be shared because {} is in use",
                    template.templateSchema(), customConverters ? "'converters'" : "key augmentation by the topic naming strategy");
            return null;
        }
        LOG.info("Schema template enabled: tenant tables with the same structure as in schema '{}' share one event schema",
                template.templateSchema());
        return template;
    }

    @Override
    public void close() {
        // Close schema storage
        schemasByTableId.close();

        // Close table storage
        try {
            tables.getStorage().close();
        }
        catch (Exception e) {
            LOG.warn("Failed to close table storage", e);
        }
    }

    /**
     * Returns the set of table ids included in the current filter configuration.
     */
    public Set<TableId> tableIds() {
        // TODO that filtering should really be done once upon insertion
        return tables.subset(tableFilter).tableIds();
    }

    @Override
    public java.util.Collection<TableId> dataCollectionIds() {
        return tableIds();
    }

    @Override
    public void assureNonEmptySchema() {
        if (tableIds().isEmpty()) {
            LOG.warn(NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }
    }

    /**
     * Get the {@link TableSchema Schema information} for the table with the given identifier, if that table exists and
     * is included by the filter configuration.
     * <p>
     * Note that the {@link Schema} will not contain any columns that have been filtered out.
     *
     * @param id
     *            the table identifier; may be null
     * @return the schema information, or null if there is no table with the given identifier, if the identifier is
     *         null, or if the table has been excluded by the filters
     */
    @Override
    public TableSchema schemaFor(TableId id) {
        TableSchema schema = schemasByTableId.get(id);
        // If schema not found in storage (e.g., cache-only mode), try to rebuild from Table
        if (schema == null) {
            Table table = tableFor(id);
            if (table != null) {
                // Rebuild and register the schema from the table definition
                buildAndRegisterSchema(table);
                schema = schemasByTableId.get(id);
            }
        }
        return schema;
    }

    /**
     * Get the {@link Table} meta-data for the table with the given identifier, if that table exists and is
     * included by the filter configuration
     *
     * @param id the table identifier; may be null
     * @return the current table definition, or null if there is no table with the given identifier, if the identifier is null,
     *         or if the table has been excluded by the filters
     */
    public Table tableFor(TableId id) {
        return tableFilter.isIncluded(id) ? tables.forTable(id) : null;
    }

    @Override
    public boolean isHistorized() {
        return false;
    }

    protected Tables tables() {
        return tables;
    }

    protected void clearSchemas() {
        schemasByTableId.clear();
        sharedTenantSchemas.clear();
    }

    /**
     * Builds up the CDC event schema for the given table and stores it in this schema.
     */
    protected void buildAndRegisterSchema(Table table) {
        if (tableFilter.isIncluded(table.id())) {
            TableSchema schema = createSchema(table);
            schemasByTableId.put(table.id(), schema);
            DebeziumOpenLineageEmitter.emit(
                    DebeziumOpenLineageEmitter.connectorContext(taskContext.getRawConfig().asMap(), config.getConnectorName(), taskContext.getRunId()),
                    DebeziumTaskState.RUNNING,
                    List.of(extractDatasetMetadata(table)));
        }
    }

    /**
     * Creates the event schema for the given table. Tenant tables of a configured schema template that have the same
     * structure share one built schema, bound to each table's own identifier via {@link TableSchema#withId(TableId)}.
     */
    private TableSchema createSchema(Table table) {
        if (schemaSharingTemplate != null) {
            final String key = schemaSharingTemplate.sharingKey(table, columnFilter, columnMappers, customKeysMapper);
            if (key != null) {
                return sharedTenantSchemas
                        .computeIfAbsent(key, k -> schemaBuilder.create(topicNamingStrategy, table, columnFilter, columnMappers, customKeysMapper))
                        .withId(table.id());
            }
        }
        return schemaBuilder.create(topicNamingStrategy, table, columnFilter, columnMappers, customKeysMapper);
    }

    private DatasetMetadata extractDatasetMetadata(Table table) {

        List<DatasetMetadata.FieldDefinition> fieldDefinitions = table.columns().stream()
                .map(c -> new DatasetMetadata.FieldDefinition(c.name(), c.typeName(), c.comment()))
                .toList();
        return new DatasetMetadata(getIdentifier(table), INPUT, DatasetMetadata.TABLE_DATASET_TYPE, DatasetMetadata.DataStore.DATABASE, fieldDefinitions);
    }

    private String getIdentifier(Table table) {

        String dbName = config.getJdbcConfig().getDatabase() == null ? "" : config.getJdbcConfig().getDatabase();

        if (table.id().catalog() == null) {
            return dbName + "." + table.id().identifier();
        }

        return table.id().identifier();
    }

    protected void removeSchema(TableId id) {
        schemasByTableId.remove(id);
    }

    /**
     * A map of schemas by table id. Table names are stored lower-case if required as per the config.
     */
    private static class SchemasByTableId {

        private final TableMappingStorage<TableSchema> storage;

        SchemasByTableId(TableMappingStorage<TableSchema> storage) {
            this.storage = storage;
        }

        public void clear() {
            storage.clear();
        }

        public TableSchema remove(TableId tableId) {
            return storage.remove(tableId);
        }

        public TableSchema get(TableId tableId) {
            return storage.get(tableId);
        }

        public TableSchema put(TableId tableId, TableSchema updated) {
            return storage.put(tableId, updated);
        }

        public void close() {
            try {
                storage.close();
            }
            catch (Exception e) {
                LOG.warn("Failed to close schema storage", e);
            }
        }
    }

    protected TableFilter getTableFilter() {
        return tableFilter;
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }

    /**
     * Refreshes the schema content with a table constructed externally
     *
     * @param table constructed externally - typically from decoder metadata or an external signal
     */
    public void refresh(Table table) {
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(table);
        // and refresh the schema
        refreshSchema(table.id());
    }

    protected void refreshSchema(TableId id) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("refreshing DB schema for table '{}'", id);
        }
        Table table = tableFor(id);

        buildAndRegisterSchema(table);
    }
}
