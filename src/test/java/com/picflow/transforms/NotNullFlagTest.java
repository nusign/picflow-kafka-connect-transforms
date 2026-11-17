package com.picflow.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NotNullFlagTest {

    private final NotNullFlag<SinkRecord> transform = new NotNullFlag.Value<>();

    @BeforeEach
    void setUp() {
        transform.configure(Map.of("source.field", "trashed_directly", "target.field", "trashed"));
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    private static SinkRecord record(Object value) {
        return new SinkRecord("debezium.public.assets", 0, null, "as_1", null, value, 42L);
    }

    private static Map<String, Object> asset(Object trashedDirectly) {
        Map<String, Object> value = new HashMap<>();
        value.put("id", "as_1");
        value.put("trashed_directly", trashedDirectly);
        return value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> applyTo(Object value) {
        return (Map<String, Object>) transform.apply(record(value)).value();
    }

    @Test
    void trashedDirectlyTrueSetsTrashed() {
        assertEquals(true, applyTo(asset(true)).get("trashed"));
    }

    @Test
    void trashedDirectlyFalseSetsTrashed() {
        assertEquals(true, applyTo(asset(false)).get("trashed"));
    }

    @Test
    void trashedDirectlyNullSetsNotTrashed() {
        assertEquals(false, applyTo(asset(null)).get("trashed"));
    }

    @Test
    void keepsOtherFieldsAndRecordMetadata() {
        SinkRecord result = transform.apply(record(asset(true)));
        Map<?, ?> value = (Map<?, ?>) result.value();
        assertEquals("as_1", value.get("id"));
        assertEquals(true, value.get("trashed_directly"));
        assertEquals("as_1", result.key());
        assertEquals(42L, result.kafkaOffset());
    }

    @Test
    void doesNotChangeTheIncomingValue() {
        Map<String, Object> value = asset(true);
        transform.apply(record(value));
        assertFalse(value.containsKey("trashed"));
    }

    @Test
    void passesThroughValueWithoutSourceField() {
        SinkRecord record = record(new HashMap<>(Map.of("id", "as_1")));
        assertSame(record, transform.apply(record));
    }

    @Test
    void passesThroughTombstone() {
        SinkRecord record = record(null);
        assertSame(record, transform.apply(record));
    }

    @Test
    void rejectsMissingConfig() {
        NotNullFlag<SinkRecord> unconfigured = new NotNullFlag.Value<>();
        assertThrows(ConfigException.class, () -> unconfigured.configure(Map.of("source.field", "trashed_directly")));
    }

    @Test
    void rejectsSameSourceAndTarget() {
        NotNullFlag<SinkRecord> unconfigured = new NotNullFlag.Value<>();
        assertThrows(ConfigException.class,
                () -> unconfigured.configure(Map.of("source.field", "trashed", "target.field", "trashed")));
    }
}
