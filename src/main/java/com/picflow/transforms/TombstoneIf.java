package com.picflow.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;

/**
 * Turns a record into a tombstone (key kept, value null) if any rule matches, e.g. {@code deleted_at != null}
 * or {@code cover == true}. With {@code behavior.on.null.values=delete} the sink then deletes the document.
 * Works on schemaless values (maps). A field missing from the value never matches.
 */
public abstract class TombstoneIf<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String IF_NOT_NULL = "if.not.null";
    public static final String IF_TRUE = "if.true";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(IF_NOT_NULL, ConfigDef.Type.LIST, List.of(), ConfigDef.Importance.HIGH,
                    "Fields that make the record a tombstone if any of them is non-null.")
            .define(IF_TRUE, ConfigDef.Type.LIST, List.of(), ConfigDef.Importance.HIGH,
                    "Boolean fields that make the record a tombstone if any of them is true.");

    private List<String> ifNotNull;
    private List<String> ifTrue;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        Map<String, Object> parsed = CONFIG_DEF.parse(configs);
        ifNotNull = (List<String>) parsed.get(IF_NOT_NULL);
        ifTrue = (List<String>) parsed.get(IF_TRUE);
        if (ifNotNull.isEmpty() && ifTrue.isEmpty()) {
            throw new ConfigException("Set at least one of " + IF_NOT_NULL + " or " + IF_TRUE);
        }
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Map<?, ?> value) || !matches(value)) {
            return record;
        }
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null,
                null,
                record.timestamp(),
                record.headers()
        );
    }

    private boolean matches(Map<?, ?> value) {
        return ifNotNull.stream().anyMatch(field -> value.get(field) != null)
                || ifTrue.stream().anyMatch(field -> Boolean.TRUE.equals(value.get(field)));
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    public static class Value<R extends ConnectRecord<R>> extends TombstoneIf<R> {}
}
