# picflow-kafka-connect-transforms

Custom Kafka Connect **Single Message Transforms (SMTs)** for the OpenSearch sink connectors. All transforms ship in one jar.

| Transform | What it does |
| --- | --- |
| `com.picflow.transforms.JsonStringToMap$Value` | Parses a JSON string field (e.g. `version_data`) into a nested map |
| `com.picflow.transforms.NotNullFlag$Value` | Sets a boolean field to whether another field is non-null (e.g. `trashed = trashed_directly != null`) |

## JsonStringToMap

Converts a JSON string field into a nested map, with legacy fixes for `version_data.data`:
- `"60000/1001"` in `frame_rate` → `59.94` (float)
- `"N/A"` in `frame_count` → `0`

| Config | Description |
| --- | --- |
| `field.name` | Field containing the JSON string |

## NotNullFlag

Sets `target.field` to `true` if `source.field` is non-null, `false` if it is null. Records without `source.field` (and tombstones) pass through unchanged.

| Config | Description |
| --- | --- |
| `source.field` | Field whose null-ness is checked |
| `target.field` | Boolean field to set |

## Configuration example

```properties
transforms=ExtractKey,ExtractAfterField,JsonStringToMapTransform,TrashedFlag

transforms.ExtractKey.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.ExtractKey.field=id

transforms.ExtractAfterField.type=org.apache.kafka.connect.transforms.ExtractField$Value
transforms.ExtractAfterField.field=after

transforms.JsonStringToMapTransform.type=com.picflow.transforms.JsonStringToMap$Value
transforms.JsonStringToMapTransform.field.name=version_data
transforms.JsonStringToMapTransform.predicate=isAssetsTopic

transforms.TrashedFlag.type=com.picflow.transforms.NotNullFlag$Value
transforms.TrashedFlag.source.field=trashed_directly
transforms.TrashedFlag.target.field=trashed
transforms.TrashedFlag.predicate=isAssetsTopic

predicates=isAssetsTopic
predicates.isAssetsTopic.type=org.apache.kafka.connect.transforms.predicates.TopicNameMatches
predicates.isAssetsTopic.pattern=debezium.public.assets
```

## Adding a transform

1. Add a class implementing `org.apache.kafka.connect.transforms.Transformation` under `src/main/java/com/picflow/transforms/`, with a nested `Value` class.
2. Register the `Value` class in `src/main/resources/META-INF/services/org.apache.kafka.connect.transforms.Transformation`; Kafka Connect doesn't find it otherwise.
3. Add a test under `src/test/java`.

## Build and release

```
mvn clean package
```

Kafka Connect loads the jar from the GitHub archive of a tag (`picflow-backend`, `kustomization/components/kafka-connect/kafka-connect-cluster.yaml`), so the built jar in `target/` is committed. To release: bump `version` in `pom.xml`, build, commit, tag the commit with the version, then point the plugin URL in `picflow-backend` at the new tag.

Without a local JDK:

```
docker run --rm -v "$PWD":/src -v "$HOME/.m2":/root/.m2 -w /src maven:3.9-eclipse-temurin-17 mvn -B clean package
```
