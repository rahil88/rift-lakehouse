# Data contracts

Versioned JSON Schemas that every event must satisfy **before** it enters Kafka.
Valid events go to `lol.match.raw`; invalid ones go to `lol.match.dlq` with the error reason,
schema version, and timestamp.

The schemas are plain JSON, so they don't depend on any language. The Python producer
validates against them today, and any other producer or consumer could load the same files.

Planned for Phase 2:

```
contracts/
└── v1/
    ├── match.schema.json
    ├── timeline_event.schema.json
    └── player_rank.schema.json
```
