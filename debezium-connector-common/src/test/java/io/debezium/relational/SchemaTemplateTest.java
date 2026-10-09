/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.relational.Tables.ColumnNameFilterFactory;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.schema.FieldNameSelector;
import io.debezium.schema.FieldNameSelector.FieldNamer;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Unit tests for {@link SchemaTemplate}, the field name cache and the related configuration validation.
 */
public class SchemaTemplateTest {

    private final SchemaTemplate template = new SchemaTemplate(Pattern.compile("^orga_[0-9]+$"), "orga_1");

    @Test
    public void identicalTenantTablesShareTheSameKey() {
        final Table template1 = payment("orga_1", false);
        final Table tenant42 = payment("orga_42", false);

        final String key1 = template.sharingKey(template1, null, null, null);
        assertThat(key1).isNotNull();
        assertThat(template.sharingKey(tenant42, null, null, null)).isEqualTo(key1);
    }

    @Test
    public void schemaQualifiedDefaultsAreNormalized() {
        // nextval('orga_42.payment_id_seq') must compare equal to nextval('orga_1.payment_id_seq')
        final Table template1 = payment("orga_1", false);
        final Table tenant4 = payment("orga_4", false);
        assertThat(template.sharingKey(tenant4, null, null, null)).isEqualTo(template.sharingKey(template1, null, null, null));
    }

    @Test
    public void differentColumnOrderGetsADifferentKey() {
        assertThat(template.sharingKey(payment("orga_7", true), null, null, null))
                .isNotEqualTo(template.sharingKey(payment("orga_1", false), null, null, null));
    }

    @Test
    public void differentColumnDefinitionGetsADifferentKey() {
        final Table base = payment("orga_8", false);
        final Table widened = base.edit()
                .addColumn(base.columnWithName("label").edit().length(255).create())
                .create();
        assertThat(template.sharingKey(widened, null, null, null)).isNotEqualTo(template.sharingKey(base, null, null, null));

        final Table nullable = base.edit()
                .addColumn(base.columnWithName("amount").edit().optional(true).create())
                .create();
        assertThat(template.sharingKey(nullable, null, null, null)).isNotEqualTo(template.sharingKey(base, null, null, null));
    }

    @Test
    public void differentPrimaryKeyGetsADifferentKey() {
        final Table base = payment("orga_9", false);
        final Table otherPk = base.edit().setPrimaryKeyNames("label").create();
        assertThat(template.sharingKey(otherPk, null, null, null)).isNotEqualTo(template.sharingKey(base, null, null, null));
    }

    @Test
    public void differentTableNameGetsADifferentKey() {
        final Table base = payment("orga_9", false);
        final Table other = base.edit().tableId(new TableId(null, "orga_9", "refund")).create();
        assertThat(template.sharingKey(other, null, null, null)).isNotEqualTo(template.sharingKey(base, null, null, null));
    }

    @Test
    public void nonTenantSchemaIsNotShared() {
        assertThat(template.sharingKey(payment("public", false), null, null, null)).isNull();
        assertThat(template.sharingKey(payment("orga_sandbox", false), null, null, null)).isNull();
        assertThat(template.sharingKey(payment("orga_123_failed_123", false), null, null, null)).isNull();
    }

    @Test
    public void tableWithAColumnMapperIsNotShared() {
        final ColumnMappers mappers = ColumnMappers.build().maskStrings("orga_42.payment.label", 5).build();
        assertThat(template.sharingKey(payment("orga_42", false), null, mappers, null)).isNull();
        // the mapper only targets orga_42, other tenants still share
        assertThat(template.sharingKey(payment("orga_43", false), null, mappers, null)).isNotNull();
    }

    @Test
    public void capturedColumnsArePartOfTheKey() {
        final Table tenant = payment("orga_42", false);
        final Tables.ColumnNameFilter excludeLabel = ColumnNameFilterFactory.createExcludeListFilter("orga_42.payment.label",
                ColumnFilterMode.SCHEMA);
        assertThat(template.sharingKey(tenant, excludeLabel, null, null)).isNotEqualTo(template.sharingKey(tenant, null, null, null));
    }

    @Test
    public void tenantSchemaMatching() {
        assertThat(template.isTenantSchema("orga_1")).isTrue();
        assertThat(template.isTenantSchema("orga_1234567890")).isTrue();
        assertThat(template.isTenantSchema("orga_")).isFalse();
        assertThat(template.isTenantSchema("public")).isFalse();
        assertThat(template.isTenantSchema(null)).isFalse();
    }

    @Test
    public void tableSchemaWithIdKeepsSchemasAndGenerators() {
        final Schema value = SchemaBuilder.struct().name("v").field("id", Schema.INT32_SCHEMA).build();
        final TableSchema shared = new TableSchema(new TableId(null, "orga_1", "payment"), null, null, null, value, null);

        assertThat(shared.withId(new TableId(null, "orga_1", "payment"))).isSameAs(shared);
        final TableSchema bound = shared.withId(new TableId(null, "orga_42", "payment"));
        assertThat(bound.id()).isEqualTo(new TableId(null, "orga_42", "payment"));
        assertThat(bound.valueSchema()).isSameAs(value);
    }

    @Test
    public void fieldNameDependsOnlyOnColumnName() {
        final FieldNamer<Column> namer = FieldNameSelector.defaultSelector(SchemaNameAdjuster.NO_OP);
        final Column a = Column.editor().name("amount").type("int4").jdbcType(Types.INTEGER).position(1).create();
        final Column b = Column.editor().name("amount").type("int4").jdbcType(Types.INTEGER).position(7).optional(false).create();
        assertThat(namer.fieldNameFor(a)).isEqualTo("amount");
        assertThat(namer.fieldNameFor(b)).isEqualTo("amount");
        // many distinct Column instances with few distinct names must keep working (no per-Column caching)
        for (int i = 0; i < 50_000; i++) {
            final Column c = Column.editor().name("col" + (i % 20)).type("int4").jdbcType(Types.INTEGER).position(i + 1).create();
            assertThat(namer.fieldNameFor(c)).isEqualTo("col" + (i % 20));
        }
    }

    @Test
    public void configValidation() {
        assertThat(problems(null, null)).isZero();
        assertThat(problems("^orga_[0-9]+$", "orga_1")).isZero();
        assertThat(problems("^orga_[0-9]+$", null)).isPositive();
        assertThat(problems(null, "orga_1")).isPositive();
        assertThat(problems("^orga_[0-9]+$", "public")).isPositive();
        assertThat(problems("^orga_[", "orga_1")).isPositive();
    }

    @Test
    public void templateIsResolvedFromConfig() {
        assertThat(SchemaTemplate.from(config(null, null))).isNull();
        final SchemaTemplate resolved = SchemaTemplate.from(config("^orga_[0-9]+$", "orga_1"));
        assertThat(resolved).isNotNull();
        assertThat(resolved.templateSchema()).isEqualTo("orga_1");
        assertThat(resolved.isTenantSchema("orga_9")).isTrue();
    }

    /**
     * A "payment" table as found in every tenant schema, with a schema-qualified sequence default.
     */
    private static Table payment(String schema, boolean swapLabelAndAmount) {
        final Column id = Column.editor().name("id").type("int4").jdbcType(Types.INTEGER).optional(false)
                .defaultValueExpression("nextval('" + schema + ".payment_id_seq'::regclass)").create();
        final Column amount = Column.editor().name("amount").type("numeric").jdbcType(Types.NUMERIC).length(12).scale(2)
                .optional(false).create();
        final Column label = Column.editor().name("label").type("varchar").jdbcType(Types.VARCHAR).length(64).optional(true)
                .create();
        final List<Column> columns = swapLabelAndAmount ? List.of(id, label, amount) : List.of(id, amount, label);
        return Table.editor()
                .tableId(new TableId(null, schema, "payment"))
                .addColumns(columns)
                .setPrimaryKeyNames("id")
                .create();
    }

    private static int problems(String pattern, String target) {
        final int[] count = { 0 };
        RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_TARGET.validate(rawConfig(pattern, target), (field, value, problem) -> count[0]++);
        return count[0];
    }

    private static Configuration rawConfig(String pattern, String target) {
        final Configuration.Builder builder = Configuration.create().with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, "core");
        if (pattern != null) {
            builder.with(RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_PATTERN, pattern);
        }
        if (target != null) {
            builder.with(RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_TARGET, target);
        }
        return builder.build();
    }

    private static RelationalDatabaseConnectorConfig config(String pattern, String target) {
        return new RelationalDatabaseConnectorConfig(rawConfig(pattern, target), null, null, 0, ColumnFilterMode.CATALOG, true) {
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
