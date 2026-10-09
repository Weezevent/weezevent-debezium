/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.PostgresConnectorConfig.SnapshotMode;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseSchema;

/**
 * Multi-tenant schema template: one schema per tenant with the same tables. Only the template schema is introspected
 * at startup, the other tenant tables are loaded from the replication stream, and structurally identical tenant
 * tables share one event schema while a tenant whose columns are in another order keeps its own.
 */
public class SchemaTemplateIT extends AbstractRecordsProducerTest {

    private static final String SETUP = "CREATE SCHEMA orga_1;" +
            "CREATE SCHEMA orga_2;" +
            "CREATE SCHEMA orga_3;" +
            "CREATE SCHEMA orga_sandbox;" +
            "CREATE TABLE orga_1.payment (id SERIAL PRIMARY KEY, amount NUMERIC(12,2) NOT NULL, label VARCHAR(64));" +
            "CREATE TABLE orga_2.payment (id SERIAL PRIMARY KEY, amount NUMERIC(12,2) NOT NULL, label VARCHAR(64));" +
            // same columns, different physical order
            "CREATE TABLE orga_3.payment (id SERIAL PRIMARY KEY, label VARCHAR(64), amount NUMERIC(12,2) NOT NULL);" +
            "CREATE TABLE orga_sandbox.payment (id SERIAL PRIMARY KEY, amount NUMERIC(12,2) NOT NULL, label VARCHAR(64));";

    @BeforeEach
    void before() throws SQLException {
        TestHelper.dropAllSchemas();
        TestHelper.execute(SETUP);
    }

    @AfterEach
    void after() throws SQLException {
        stopConnector();
        TestHelper.dropAllSchemas();
    }

    @Test
    void shouldStreamTenantsFromTemplateWithCorrectValues() throws Exception {
        final LogInterceptor schemaLogs = new LogInterceptor(PostgresSchema.class);
        final LogInterceptor sharingLogs = new LogInterceptor(RelationalDatabaseSchema.class);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "^orga_[0-9]+$")
                .with(RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_PATTERN, "^orga_[0-9]+$")
                .with(RelationalDatabaseConnectorConfig.SCHEMA_TEMPLATE_CANONICALIZATION_TARGET, "orga_1")
                .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();

        assertThat(schemaLogs.containsMessage("reading the initial table structure from schema 'orga_1' only")).isTrue();
        assertThat(sharingLogs.containsMessage("tenant tables with the same structure as in schema 'orga_1' share one event schema")).isTrue();

        TestHelper.execute("INSERT INTO orga_1.payment (amount, label) VALUES (10.50, 'one');");
        TestHelper.execute("INSERT INTO orga_2.payment (amount, label) VALUES (20.25, 'two');");
        TestHelper.execute("INSERT INTO orga_3.payment (label, amount) VALUES ('three', 30.75);");
        TestHelper.execute("INSERT INTO orga_sandbox.payment (amount, label) VALUES (99.99, 'excluded');");
        TestHelper.execute("UPDATE orga_2.payment SET amount = 21.00, label = 'two-updated' WHERE id = 1;");
        TestHelper.execute("INSERT INTO orga_3.payment (label, amount) VALUES ('three-bis', 31.00);");

        final int expected = 5;
        final TestConsumer consumer = testConsumer(expected, "orga_");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final Map<String, List<SourceRecord>> byTopic = recordsByTopic(expected, consumer);

        // every tenant keeps its own topic, the excluded schema is not captured
        assertThat(byTopic.keySet()).containsExactlyInAnyOrder(
                "test_server.orga_1.payment", "test_server.orga_2.payment", "test_server.orga_3.payment");

        final SourceRecord one = byTopic.get("test_server.orga_1.payment").get(0);
        final SourceRecord two = byTopic.get("test_server.orga_2.payment").get(0);
        final SourceRecord twoUpdated = byTopic.get("test_server.orga_2.payment").get(1);
        final SourceRecord three = byTopic.get("test_server.orga_3.payment").get(0);
        final SourceRecord threeBis = byTopic.get("test_server.orga_3.payment").get(1);

        assertRow(one, "orga_1", 1, "10.50", "one");
        assertRow(two, "orga_2", 1, "20.25", "two");
        assertRow(twoUpdated, "orga_2", 1, "21.00", "two-updated");
        assertRow(three, "orga_3", 1, "30.75", "three");
        assertRow(threeBis, "orga_3", 2, "31.00", "three-bis");

        // orga_2 has the same structure as orga_1: same event schema instance, shared
        assertThat(two.valueSchema()).isSameAs(one.valueSchema());
        assertThat(two.keySchema()).isSameAs(one.keySchema());
        // orga_3 has its columns in another order: it must not reuse that schema
        assertThat(three.valueSchema()).isNotSameAs(one.valueSchema());
        assertThat(three.valueSchema().name()).isEqualTo("test_server.orga_3.payment.Envelope");
    }

    @Test
    void shouldIntrospectEveryTableWithoutTemplate() throws Exception {
        final LogInterceptor schemaLogs = new LogInterceptor(PostgresSchema.class);

        start(PostgresConnector.class, TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, SnapshotMode.NO_DATA)
                .with(PostgresConnectorConfig.SCHEMA_INCLUDE_LIST, "^orga_[0-9]+$")
                .build());
        assertConnectorIsRunning();
        waitForStreamingToStart();

        assertThat(schemaLogs.containsMessage("reading the initial table structure")).isFalse();

        TestHelper.execute("INSERT INTO orga_1.payment (amount, label) VALUES (10.50, 'one');");
        TestHelper.execute("INSERT INTO orga_3.payment (label, amount) VALUES ('three', 30.75);");

        final TestConsumer consumer = testConsumer(2, "orga_");
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);
        final Map<String, List<SourceRecord>> byTopic = recordsByTopic(2, consumer);
        assertRow(byTopic.get("test_server.orga_1.payment").get(0), "orga_1", 1, "10.50", "one");
        assertRow(byTopic.get("test_server.orga_3.payment").get(0), "orga_3", 1, "30.75", "three");
        assertThat(byTopic.get("test_server.orga_3.payment").get(0).valueSchema())
                .isNotSameAs(byTopic.get("test_server.orga_1.payment").get(0).valueSchema());
    }

    private static void assertRow(SourceRecord record, String schema, int id, String amount, String label) {
        final Struct value = (Struct) record.value();
        final Struct after = value.getStruct("after");
        assertThat(value.getStruct("source").getString("schema")).isEqualTo(schema);
        assertThat(value.getStruct("source").getString("table")).isEqualTo("payment");
        assertThat(after.getInt32("id")).isEqualTo(id);
        assertThat((BigDecimal) after.get("amount")).isEqualByComparingTo(new BigDecimal(amount));
        assertThat(after.getString("label")).isEqualTo(label);
        assertThat(((Struct) record.key()).getInt32("id")).isEqualTo(id);
        assertThat(record.topic()).isEqualTo("test_server." + schema + ".payment");
    }
}
