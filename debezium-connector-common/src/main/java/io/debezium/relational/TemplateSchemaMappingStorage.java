/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.common.annotation.Incubating;

/**
 * A {@link TableMappingStorage} implementation for multi-tenant databases that use one DDL-identical schema per tenant.
 * <p>
 * When the configured {@code schema.template.canonicalization.pattern} and {@code schema.template.canonicalization.target}
 * are set, every {@link TableId} whose schema part matches the pattern has its schema rewritten to the target schema
 * before being stored or looked up. As a result a single relational model (and the derived {@link TableSchema}) is kept
 * for a logical table regardless of how many physical schemas contain it. This avoids building and retaining one model
 * per tenant schema, which for databases with hundreds of thousands of DDL-identical tables is prohibitively expensive.
 * <p>
 * When canonicalization is not configured, this storage behaves exactly like {@link ConcurrentMapTableMappingStorage}
 * (optionally applying case-insensitive normalization), so it is safe to use for connectors that do not need the
 * template behavior.
 *
 * @param <V> the type of values stored
 * @author Debezium Authors
 */
@Incubating
public class TemplateSchemaMappingStorage<V> implements TableMappingStorage<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateSchemaMappingStorage.class);

    private final ConcurrentMap<TableId, V> storage;

    private boolean tableIdCaseInsensitive;
    private Pattern schemaPattern;
    private String targetSchema;

    /**
     * Creates a new storage instance.
     * Must call {@link #configure(RelationalDatabaseConnectorConfig, boolean, Type)} before use.
     */
    public TemplateSchemaMappingStorage() {
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public void configure(RelationalDatabaseConnectorConfig config, boolean tableIdCaseInsensitive, Type type) {
        this.tableIdCaseInsensitive = tableIdCaseInsensitive;

        if (config == null) {
            // No connector configuration available (e.g. transient Tables instances); fall back to pass-through.
            return;
        }

        final String pattern = config.getSchemaTemplateCanonicalizationPattern();
        final String target = config.getSchemaTemplateCanonicalizationTarget();

        if (isBlank(pattern) != isBlank(target)) {
            throw new IllegalArgumentException(
                    "Both 'schema.template.canonicalization.pattern' and 'schema.template.canonicalization.target' must be "
                            + "provided together, or neither of them.");
        }

        if (!isBlank(pattern)) {
            this.schemaPattern = Pattern.compile(pattern);
            this.targetSchema = target;
            LOGGER.info("Schema template canonicalization enabled for {} storage: schemas matching '{}' are mapped to '{}'",
                    type, pattern, target);
        }
    }

    @Override
    public V get(TableId tableId) {
        return storage.get(canonicalize(tableId));
    }

    @Override
    public V put(TableId tableId, V value) {
        return storage.put(canonicalize(tableId), value);
    }

    @Override
    public V remove(TableId tableId) {
        return storage.remove(canonicalize(tableId));
    }

    @Override
    public void clear() {
        storage.clear();
    }

    @Override
    public int size() {
        return storage.size();
    }

    @Override
    public boolean isEmpty() {
        return storage.isEmpty();
    }

    @Override
    public Set<TableId> keySet() {
        return storage.keySet();
    }

    @Override
    public void forEach(BiConsumer<? super TableId, ? super V> action) {
        storage.forEach(action);
    }

    @Override
    public void close() {
        // No-op: in-memory storage doesn't require cleanup.
    }

    /**
     * Rewrites the schema part of the given identifier to the target schema when it matches the configured pattern,
     * then applies case-insensitive normalization if required. When canonicalization is not configured the identifier
     * is returned unchanged (apart from case normalization).
     *
     * @param tableId the table identifier to canonicalize; may be null
     * @return the canonicalized table identifier, or null if the input was null
     */
    private TableId canonicalize(TableId tableId) {
        if (tableId == null) {
            return null;
        }

        TableId result = tableId;
        if (schemaPattern != null) {
            final String schema = tableId.schema();
            if (schema != null && schemaPattern.matcher(schema).matches() && !targetSchema.equals(schema)) {
                result = new TableId(tableId.catalog(), targetSchema, tableId.table());
            }
        }

        return tableIdCaseInsensitive ? result.toLowercase() : result;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
