/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.TableMappingStorage.Type;

/**
 * Unit tests for {@link TemplateSchemaMappingStorage}, which rewrites the schema part of a {@link TableId} to a
 * single template schema so that DDL-identical per-tenant schemas share one stored value.
 *
 * @author Debezium Authors
 */
public class TemplateSchemaMappingStorageTest {

    private static final String PATTERN = "^orga_[0-9]+$";
    private static final String TARGET = "orga_1";

    @Test
    public void shouldCanonicalizeMatchingSchemasToTargetOnPut() {
        final TemplateSchemaMappingStorage<String> storage = newStorage(PATTERN, TARGET);

        storage.put(tableId("orga_1", "payment"), "model-from-orga-1");
        // A different tenant schema for the same logical table must resolve to the same stored entry.
        final String previous = storage.put(tableId("orga_42", "payment"), "model-from-orga-42");

        assertThat(previous).isEqualTo("model-from-orga-1");
        assertThat(storage.size()).isEqualTo(1);
        assertThat(storage.keySet()).containsExactly(tableId("orga_1", "payment"));
    }

    @Test
    public void shouldResolveLookupForAnyMatchingSchema() {
        final TemplateSchemaMappingStorage<String> storage = newStorage(PATTERN, TARGET);

        storage.put(tableId("orga_1", "payment"), "model");

        assertThat(storage.get(tableId("orga_1", "payment"))).isEqualTo("model");
        assertThat(storage.get(tableId("orga_42", "payment"))).isEqualTo("model");
        assertThat(storage.get(tableId("orga_99999", "payment"))).isEqualTo("model");
    }

    @Test
    public void shouldKeepDistinctLogicalTablesSeparate() {
        final TemplateSchemaMappingStorage<String> storage = newStorage(PATTERN, TARGET);

        storage.put(tableId("orga_7", "payment"), "payment-model");
        storage.put(tableId("orga_7", "ticket"), "ticket-model");

        assertThat(storage.size()).isEqualTo(2);
        assertThat(storage.get(tableId("orga_1", "payment"))).isEqualTo("payment-model");
        assertThat(storage.get(tableId("orga_1", "ticket"))).isEqualTo("ticket-model");
    }

    @Test
    public void shouldNotCanonicalizeNonMatchingSchemas() {
        final TemplateSchemaMappingStorage<String> storage = newStorage(PATTERN, TARGET);

        storage.put(tableId("public", "payment"), "public-model");
        storage.put(tableId("orga_sandbox", "payment"), "sandbox-model");

        // Neither schema matches the pattern, so both are stored under their own identifier.
        assertThat(storage.size()).isEqualTo(2);
        assertThat(storage.get(tableId("public", "payment"))).isEqualTo("public-model");
        assertThat(storage.get(tableId("orga_sandbox", "payment"))).isEqualTo("sandbox-model");
    }

    @Test
    public void shouldRemoveUsingCanonicalKey() {
        final TemplateSchemaMappingStorage<String> storage = newStorage(PATTERN, TARGET);

        storage.put(tableId("orga_1", "payment"), "model");
        final String removed = storage.remove(tableId("orga_500", "payment"));

        assertThat(removed).isEqualTo("model");
        assertThat(storage.isEmpty()).isTrue();
    }

    @Test
    public void shouldBehaveAsPlainStorageWhenCanonicalizationDisabled() {
        final TemplateSchemaMappingStorage<String> storage = newStorage(null, null);

        storage.put(tableId("orga_1", "payment"), "model-1");
        storage.put(tableId("orga_42", "payment"), "model-42");

        // Without canonicalization each physical schema keeps its own entry (default behavior).
        assertThat(storage.size()).isEqualTo(2);
        assertThat(storage.get(tableId("orga_1", "payment"))).isEqualTo("model-1");
        assertThat(storage.get(tableId("orga_42", "payment"))).isEqualTo("model-42");
    }

    @Test
    public void shouldToleratePassThroughConfigureWithoutConfig() {
        final TemplateSchemaMappingStorage<String> storage = new TemplateSchemaMappingStorage<>();
        storage.configure(null, false, Type.SCHEMAS);

        storage.put(tableId("orga_1", "payment"), "model-1");
        storage.put(tableId("orga_42", "payment"), "model-42");

        assertThat(storage.size()).isEqualTo(2);
    }

    @Test
    public void shouldRejectPatternWithoutTarget() {
        assertThatThrownBy(() -> newStorage(PATTERN, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shouldRejectTargetWithoutPattern() {
        assertThatThrownBy(() -> newStorage(null, TARGET))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static TableId tableId(String schema, String table) {
        return new TableId(null, schema, table);
    }

    private static TemplateSchemaMappingStorage<String> newStorage(String pattern, String target) {
        final TemplateSchemaMappingStorage<String> storage = new TemplateSchemaMappingStorage<>();
        storage.configure(buildConfig(pattern, target), false, Type.SCHEMAS);
        return storage;
    }

    private static RelationalDatabaseConnectorConfig buildConfig(String pattern, String target) {
        final Configuration.Builder builder = Configuration.create()
                .with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core");
        if (pattern != null) {
            builder.with(RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_PATTERN, pattern);
        }
        if (target != null) {
            builder.with(RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_TARGET, target);
        }

        return new RelationalDatabaseConnectorConfig(builder.build(), null, null, 0, ColumnFilterMode.CATALOG, true) {
            @Override
            protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
                return null;
            }

            @Override
            public String getContextName() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public EnumeratedValue getSnapshotMode() {
                return null;
            }

            @Override
            public Optional<EnumeratedValue> getSnapshotLockingMode() {
                return Optional.empty();
            }
        };
    }
}
