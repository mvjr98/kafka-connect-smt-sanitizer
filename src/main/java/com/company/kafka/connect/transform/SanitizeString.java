package com.company.kafka.connect.transform;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SanitizeString<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String DEBEZIUM_UNAVAILABLE_VALUE = "__debezium_unavailable_value";

    private Set<String> fieldsToSanitize;
    private List<Character> charsToRemove;
    private boolean replaceDebeziumUnavailable;
    private final Map<Schema, Schema> schemaCache = Collections.synchronizedMap(new HashMap<>());

    @Override
    public void configure(Map<String, ?> props) {
        SanitizeStringConfig config = new SanitizeStringConfig(props);

        List<String> fields = config.getFields();
        if (fields == null || fields.isEmpty() || (fields.size() == 1 && fields.get(0).isEmpty())) {
            fieldsToSanitize = null;
        } else {
            Set<String> normalized = new HashSet<>();
            for (String field : fields) {
                if (field != null) {
                    String trimmed = field.trim();
                    if (!trimmed.isEmpty()) {
                        normalized.add(trimmed);
                    }
                }
            }
            fieldsToSanitize = normalized.isEmpty() ? null : normalized;
        }

        List<String> patterns = config.getPatterns();
        List<Character> toRemove = new ArrayList<>();
        for (String pattern : patterns) {
            if (pattern == null) {
                continue;
            }
            String hex = pattern.trim();
            if (hex.isEmpty()) {
                continue;
            }
            int codePoint = Integer.parseInt(hex, 16);
            if (!Character.isValidCodePoint(codePoint)) {
                throw new IllegalArgumentException("Invalid hex code point: " + hex);
            }
            if (codePoint <= Character.MAX_VALUE) {
                toRemove.add((char) codePoint);
            }
        }
        charsToRemove = toRemove;
        replaceDebeziumUnavailable = config.replaceDebeziumUnavailableValue();
    }

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null) {
            return record;
        }

        Schema valueSchema = record.valueSchema();
        Object value = record.value();

        if (valueSchema == null) {
            if (!(value instanceof Map)) {
                return record;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> valueMap = (Map<String, Object>) value;
            Map<String, Object> sanitized = sanitizeMap(valueMap);
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                null,
                sanitized,
                record.timestamp(),
                record.headers()
            );
        }

        if (!(value instanceof Struct)) {
            return record;
        }

        Struct valueStruct = (Struct) value;
        Schema targetSchema = schemaCache.computeIfAbsent(valueSchema, this::copySchema);
        Struct sanitizedStruct = sanitizeStruct(valueStruct, targetSchema);

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            targetSchema,
            sanitizedStruct,
            record.timestamp(),
            record.headers()
        );
    }

    private Schema copySchema(Schema source) {
        if (source == null) {
            return null;
        }
        if (source.type() != Schema.Type.STRUCT) {
            return source;
        }

        SchemaBuilder builder = SchemaBuilder.struct();

        if (source.name() != null) {
            builder.name(source.name());
        }
        if (source.version() != null) {
            builder.version(source.version());
        }
        if (source.doc() != null) {
            builder.doc(source.doc());
        }
        Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }
        if (source.isOptional()) {
            builder.optional();
        }
        Object defaultValue = source.defaultValue();
        if (defaultValue != null) {
            builder.defaultValue(defaultValue);
        }

        for (Field field : source.fields()) {
            Schema fieldSchema = field.schema();
            Schema copied = fieldSchema.type() == Schema.Type.STRUCT ? copySchema(fieldSchema) : fieldSchema;
            builder.field(field.name(), copied);
        }

        return builder.build();
    }

    private Struct sanitizeStruct(Struct source, Schema targetSchema) {
        Struct target = new Struct(targetSchema);

        for (Field targetField : targetSchema.fields()) {
            String fieldName = targetField.name();
            Object raw = source.get(fieldName);
            if (raw == null) {
                target.put(fieldName, null);
                continue;
            }

            if (replaceDebeziumUnavailable && DEBEZIUM_UNAVAILABLE_VALUE.equals(raw)) {
                target.put(fieldName, null);
                continue;
            }

            Schema fieldSchema = targetField.schema();
            switch (fieldSchema.type()) {
                case STRING:
                    if (shouldSanitizeField(fieldName)) {
                        target.put(fieldName, sanitizeString((String) raw));
                    } else {
                        target.put(fieldName, raw);
                    }
                    break;
                case STRUCT:
                    target.put(fieldName, sanitizeStruct((Struct) raw, fieldSchema));
                    break;
                default:
                    target.put(fieldName, raw);
            }
        }

        return target;
    }

    private Map<String, Object> sanitizeMap(Map<String, Object> source) {
        Map<String, Object> target = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String fieldName = entry.getKey();
            Object raw = entry.getValue();

            if (raw == null) {
                target.put(fieldName, null);
            } else if (replaceDebeziumUnavailable && DEBEZIUM_UNAVAILABLE_VALUE.equals(raw)) {
                target.put(fieldName, null);
            } else if (raw instanceof String) {
                target.put(fieldName, shouldSanitizeField(fieldName) ? sanitizeString((String) raw) : raw);
            } else if (raw instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> nested = (Map<String, Object>) raw;
                target.put(fieldName, sanitizeMap(nested));
            } else {
                target.put(fieldName, raw);
            }
        }
        return target;
    }

    private boolean shouldSanitizeField(String fieldName) {
        return fieldsToSanitize == null || fieldsToSanitize.contains(fieldName);
    }

    private String sanitizeString(String value) {
        String result = value;
        for (char c : charsToRemove) {
            result = result.replace(String.valueOf(c), "");
        }
        return result;
    }

    @Override
    public ConfigDef config() {
        return SanitizeStringConfig.CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaCache.clear();
    }
}
