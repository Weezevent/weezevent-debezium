/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.relational;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import io.debezium.relational.Key.KeyMapper;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.mapping.ColumnMappers;
import io.debezium.util.Strings;

/**
 * Multi-tenant schema template, configured with {@code schema.template.canonicalization.pattern} and
 * {@code schema.template.canonicalization.target}.
 * <p>
 * Tenant schemas match the pattern and share the table definitions of the template (target) schema. This class
 * tells which schemas are tenant schemas and computes a structural key for a tenant table, so that tenant tables
 * with an identical structure can share one event schema. The key covers everything the event schema is derived
 * from (columns, their order, types, optionality, defaults, primary and message key, captured columns), so two
 * tables only share a schema when that schema is exactly what each of them would have built on its own.
 */
public final class SchemaTemplate {

    private static final char SEP = '\u0001';

    private final Pattern tenantSchemaPattern;
    private final String templateSchema;

    SchemaTemplate(Pattern tenantSchemaPattern, String templateSchema) {
        this.tenantSchemaPattern = Objects.requireNonNull(tenantSchemaPattern);
        this.templateSchema = Objects.requireNonNull(templateSchema);
    }

    /**
     * @return the configured template, or {@code null} if no template is configured
     */
    public static SchemaTemplate from(RelationalDatabaseConnectorConfig config) {
        final String pattern = config.getSchemaTemplateCanonicalizationPattern();
        final String target = config.getSchemaTemplateCanonicalizationTarget();
        if (Strings.isNullOrBlank(pattern) || Strings.isNullOrBlank(target)) {
            return null;
        }
        return new SchemaTemplate(Pattern.compile(pattern), target);
    }

    /**
     * @return the name of the template schema
     */
    public String templateSchema() {
        return templateSchema;
    }

    /**
     * @return whether the given schema name is a tenant schema (matches the configured pattern)
     */
    public boolean isTenantSchema(String schema) {
        return schema != null && tenantSchemaPattern.matcher(schema).matches();
    }

    /**
     * Computes the key under which the event schema of a tenant table can be shared.
     *
     * @return the sharing key, or {@code null} if the table must get its own event schema (not a tenant table, or a
     *         column mapper applies to one of its columns)
     */
    public String sharingKey(Table table, ColumnNameFilter columnFilter, ColumnMappers columnMappers, KeyMapper keyMapper) {
        final TableId id = table.id();
        if (!isTenantSchema(id.schema())) {
            return null;
        }
        final String schema = id.schema();
        final StringBuilder key = new StringBuilder(512)
                .append(id.catalog()).append(SEP)
                .append(id.table()).append(SEP)
                .append(normalize(table.comment(), schema)).append(SEP)
                .append(table.defaultCharsetName()).append(SEP)
                .append(table.primaryKeyColumnNames()).append(SEP);

        final List<Column> keyColumns = new Key.Builder(table).customKeyMapper(keyMapper).build().keyColumns();
        for (Column column : keyColumns) {
            key.append(column.name()).append(',');
        }
        key.append(SEP);

        for (Column column : table.columns()) {
            // Column mappers (masking, truncation, ...) are configured per fully-qualified column, so a mapped table
            // keeps its own schema rather than risk inheriting another tenant's mapping.
            if (columnMappers != null && columnMappers.mapperFor(id, column) != null) {
                return null;
            }
            final boolean captured = columnFilter == null || columnFilter.matches(id.catalog(), schema, id.table(), column.name());
            key.append(column.name()).append(SEP)
                    .append(captured).append(SEP)
                    .append(column.position()).append(SEP)
                    .append(column.jdbcType()).append(SEP)
                    .append(column.nativeType()).append(SEP)
                    .append(column.typeName()).append(SEP)
                    .append(column.typeExpression()).append(SEP)
                    .append(column.charsetName()).append(SEP)
                    .append(column.length()).append(SEP)
                    .append(column.scale().orElse(null)).append(SEP)
                    .append(column.isOptional()).append(SEP)
                    .append(column.isAutoIncremented()).append(SEP)
                    .append(column.isGenerated()).append(SEP)
                    .append(column.hasDefaultValue()).append(SEP)
                    .append(normalize(column.defaultValueExpression().orElse(null), schema)).append(SEP)
                    .append(column.enumValues()).append(SEP)
                    .append(normalize(column.comment(), schema)).append(SEP);
        }
        return key.toString();
    }

    /**
     * Rewrites references to the tenant schema (e.g. {@code nextval('orga_42.seq'::regclass)}) to the template schema,
     * so that otherwise identical definitions compare equal across tenants.
     */
    private String normalize(String value, String tenantSchema) {
        if (value == null || tenantSchema.equals(templateSchema)) {
            return value;
        }
        return value
                .replace('"' + tenantSchema + "\".", '"' + templateSchema + "\".")
                .replace(tenantSchema + ".", templateSchema + ".");
    }
}
