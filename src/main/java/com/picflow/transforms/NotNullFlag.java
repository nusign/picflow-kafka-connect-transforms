package com.picflow.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

/**
 * Sets a boolean field to whether another field is non-null, e.g. {@code trashed = trashed_directly != null}.
 * Works on schemaless values (maps). Records without the source field are passed through unchanged,
 * so a partial value never gets a wrong {@code false}.
 */
public abstract class NotNullFlag<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String SOURCE_FIELD = "source.field";
    public static final String TARGET_FIELD = "target.field";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SOURCE_FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "Field whose null-ness is checked.")
            .define(TARGET_FIELD, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(),
                    ConfigDef.Importance.HIGH, "Boolean field to set: true if the source field is non-null.");

    private String sourceField;
    private String targetField;

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> parsed = CONFIG_DEF.parse(configs);
        sourceField = (String) parsed.get(SOURCE_FIELD);
        targetField = (String) parsed.get(TARGET_FIELD);
        if (sourceField.equals(targetField)) {
            throw new ConfigException(TARGET_FIELD + " must differ from " + SOURCE_FIELD);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        if (!(record.value() instanceof Map<?, ?> value) || !value.containsKey(sourceField)) {
            return record;
        }
        Map<String, Object> updated = new HashMap<>((Map<String, Object>) value);
        updated.put(targetField, value.get(sourceField) != null);
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                updated,
                record.timestamp(),
                record.headers()
        );
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {}

    public static class Value<R extends ConnectRecord<R>> extends NotNullFlag<R> {}
}
