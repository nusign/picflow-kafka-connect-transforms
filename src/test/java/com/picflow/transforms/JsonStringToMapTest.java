package com.picflow.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class JsonStringToMapTest {

    @Test
    void parsesWholeStringValueIntoSchemalessJson() {
        JsonStringToMap.Value<SourceRecord> transform = new JsonStringToMap.Value<>();
        transform.configure(Map.of());
        String payload = "{\"operation\": \"updated\", \"object\": {\"position\": [0.5, 0], \"text_info\": null}}";
        SourceRecord record = new SourceRecord(
                null, null, "outbox.event.watermark", null,
                Schema.STRING_SCHEMA, "wm_1", Schema.STRING_SCHEMA, payload);
        record.headers().addString("tenant_id", "tnt_1");

        SourceRecord result = transform.apply(record);

        JsonConverter converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", "false"), false);
        String json = new String(
                converter.fromConnectData(result.topic(), result.valueSchema(), result.value()),
                StandardCharsets.UTF_8);
        assertNull(result.valueSchema());
        assertEquals("{\"operation\":\"updated\",\"object\":{\"position\":[0.5,0],\"text_info\":null}}", json);
        assertEquals("tnt_1", result.headers().lastWithName("tenant_id").value());
    }
}
