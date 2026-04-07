package com.company.kafka.connect.transform;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class SanitizeStringConfig extends AbstractConfig {

    public static final String FIELDS_CONFIG = "fields";
    public static final String PATTERNS_CONFIG = "patterns";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(
            FIELDS_CONFIG,
            ConfigDef.Type.LIST,
            null,
            ConfigDef.Importance.MEDIUM,
            "Comma-separated list of fields to sanitize. If empty, all string fields are sanitized."
        )
        .define(
            PATTERNS_CONFIG,
            ConfigDef.Type.LIST,
            "00",
            ConfigDef.Importance.HIGH,
            "Comma-separated list of hex character codes to remove (example: 00,08,11)."
        );

    public SanitizeStringConfig(Map<?, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public List<String> getFields() {
        return getList(FIELDS_CONFIG);
    }

    public List<String> getPatterns() {
        return getList(PATTERNS_CONFIG);
    }
}
