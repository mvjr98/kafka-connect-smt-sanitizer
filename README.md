# Kafka Connect SMT Sanitizer

Custom Single Message Transform (SMT) for Kafka Connect that removes problematic control characters from string fields before records reach sink connectors.

## Why

PostgreSQL rejects null bytes (`0x00`) in `TEXT`/`VARCHAR`. When a sink connector writes batched statements, one bad record can fail the entire batch and stop the task.

This SMT removes configured characters early in the pipeline.

## Features

- Removes characters by configurable hex codes (`patterns`)
- Optional field allowlist (`fields`)
- Works with schema-based records (`Struct`) and schemaless records (`Map`)
- Recursively sanitizes nested structures

## Configuration

- `fields` (optional): comma-separated list of field names to sanitize
  - If omitted, all string fields are sanitized
- `patterns` (optional): comma-separated hex codes to remove
  - Default: `00`
  - Example: `00,08,11`

## Example connector snippet

```yaml
transforms: sanitize,unwrap,renameKey,renameValue

transforms.sanitize.type: com.company.kafka.connect.transform.SanitizeString
transforms.sanitize.fields: "OBSERVACOES,OBSERVACOESPUBLICACAO,OBSCANCELAMENTO,TEXTODECISOES,CONTEUDOINTIMACAO,OBSERVACAOPERICIA"
transforms.sanitize.patterns: "00,08,11"

transforms.unwrap.type: io.debezium.transforms.ExtractNewRecordState
```

## Build

```bash
mvn clean package
```

Output jar:

`target/kafka-connect-smt-sanitizer-1.0.0.jar`

## Install in Kafka Connect

1. Copy the jar to a plugin path available to workers.
2. Ensure `plugin.path` includes that directory.
3. Restart workers.
4. Update connector config with the SMT alias and settings.
