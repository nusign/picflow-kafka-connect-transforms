package com.picflow.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TombstoneIfTest {

    private final TombstoneIf<SinkRecord> transform = new TombstoneIf.Value<>();

    @BeforeEach
    void setUp() {
        transform.configure(Map.of("if.not.null", "deleted_at", "if.true", "cover"));
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    private static SinkRecord record(Object value) {
        return new SinkRecord("debezium.public.assets", 0, null, "as_1", null, value, 42L);
    }

    private static Map<String, Object> asset(Object deletedAt, Object cover) {
        Map<String, Object> value = new HashMap<>();
        value.put("id", "as_1");
        value.put("deleted_at", deletedAt);
        value.put("cover", cover);
        return value;
    }

    @Test
    void deletedAtSetMakesTombstone() {
        SinkRecord result = transform.apply(record(asset(1763395200L, false)));
        assertNull(result.value());
        assertNull(result.valueSchema());
    }

    @Test
    void coverTrueMakesTombstone() {
        assertNull(transform.apply(record(asset(null, true))).value());
    }

    @Test
    void bothRulesMatchingMakesTombstone() {
        assertNull(transform.apply(record(asset(1763395200L, true))).value());
    }

    @Test
    void tombstoneKeepsKeyAndRecordMetadata() {
        SinkRecord result = transform.apply(record(asset(1763395200L, false)));
        assertEquals("as_1", result.key());
        assertEquals("debezium.public.assets", result.topic());
        assertEquals(42L, result.kafkaOffset());
    }

    @Test
    void passesThroughLiveAsset() {
        SinkRecord record = record(asset(null, false));
        assertSame(record, transform.apply(record));
    }

    @Test
    void passesThroughCoverNull() {
        SinkRecord record = record(asset(null, null));
        assertSame(record, transform.apply(record));
    }

    @Test
    void passesThroughValueWithoutRuleFields() {
        SinkRecord record = record(new HashMap<>(Map.of("id", "as_1")));
        assertSame(record, transform.apply(record));
    }

    @Test
    void passesThroughTombstone() {
        SinkRecord record = record(null);
        assertSame(record, transform.apply(record));
    }

    @Test
    void acceptsListOfFields() {
        TombstoneIf<SinkRecord> multi = new TombstoneIf.Value<>();
        multi.configure(Map.of("if.not.null", "deleted_at, archived_at"));
        Map<String, Object> value = new HashMap<>(Map.of("id", "as_1"));
        value.put("archived_at", 1763395200L);
        assertNull(multi.apply(record(value)).value());
    }

    @Test
    void worksWithOnlyOneRuleType() {
        TombstoneIf<SinkRecord> coverOnly = new TombstoneIf.Value<>();
        coverOnly.configure(Map.of("if.true", "cover"));
        SinkRecord record = record(asset(1763395200L, false));
        assertSame(record, coverOnly.apply(record));
    }

    @Test
    void rejectsNoRules() {
        TombstoneIf<SinkRecord> unconfigured = new TombstoneIf.Value<>();
        assertThrows(ConfigException.class, () -> unconfigured.configure(Map.of()));
    }
}
