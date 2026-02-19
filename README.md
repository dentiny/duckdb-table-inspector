# DuckDB Table Inspector

A DuckDB extension that provides observability into DuckDB storage internals. It helps users understand storage usage at the database, table, and column levels, and addresses issues like unexpected file size or poor compression.

## Installation

```sql
FORCE INSTALL table_inspector FROM community;
LOAD table_inspector;
```

## Functions Overview

| Function | Description |
|----------|-------------|
| [`inspect_database()`](#inspect_database) | List all tables in a database with their persisted data size |
| [`inspect_column()`](#inspect_column) | Per-segment storage details for a specific column (compression, size) |
| [`inspect_storage()`](#inspect_storage) | List all attached persistent databases with file sizes |
| [`inspect_block_usage()`](#inspect_block_usage) | High-level storage breakdown (table data vs index vs metadata vs free blocks) |

> **Note:** Most functions require a persistent database file and do not work with in-memory databases. All functions report on checkpointed data -- run `CHECKPOINT` before inspecting to ensure the latest state is reflected.

## Usage

### `inspect_database()`

List all tables in a database with their persisted data size. Useful for identifying which tables consume the most space.

```sql
-- Inspect the current database
SELECT * FROM inspect_database();

-- Inspect a specific attached database
SELECT * FROM inspect_database('mydb');
```

| Column | Type | Description |
|--------|------|-------------|
| `database_name` | VARCHAR | Database name |
| `schema_name` | VARCHAR | Schema name |
| `table_name` | VARCHAR | Table name |
| `persisted_data_bytes` | BIGINT | Persisted data size in bytes |

### `inspect_column()`

Show per-segment storage information for a specific column -- compression type, compressed size, estimated decompressed size, and row count per row group.

```sql
-- Inspect a column in the current database
SELECT * FROM inspect_column('my_table', 'my_column');

-- Inspect with explicit database name
SELECT * FROM inspect_column('mydb', 'my_table', 'my_column');

-- Schema-qualified table names are supported
SELECT * FROM inspect_column('my_schema.my_table', 'my_column');
```

| Column | Type | Description |
|--------|------|-------------|
| `row_group_id` | BIGINT | Row group index |
| `column_name` | VARCHAR | Column name |
| `column_type` | VARCHAR | Data type (e.g., "INTEGER", "VARCHAR") |
| `compression` | VARCHAR | Compression method (e.g., "CONSTANT", "DICTIONARY", "FSST") |
| `compressed_bytes` | BIGINT | Compressed size on disk in bytes |
| `estimated_decompressed_bytes` | BIGINT | Estimated uncompressed size in bytes (NULL for variable-length types) |
| `row_count` | BIGINT | Number of rows in this segment |

### `inspect_storage()`

List all attached persistent databases with their database file and WAL file sizes.

```sql
SELECT * FROM inspect_storage();
```

| Column | Type | Description |
|--------|------|-------------|
| `database_name` | VARCHAR | Database name |
| `database_file_bytes` | BIGINT | Size of the `.duckdb` file in bytes |
| `wal_file_bytes` | BIGINT | Size of the WAL file in bytes |

### `inspect_block_usage()`

High-level storage breakdown by component type.

```sql
-- Inspect the current database
SELECT * FROM inspect_block_usage();

-- Inspect a specific attached database
SELECT * FROM inspect_block_usage('mydb');
```

| Column | Type | Description |
|--------|------|-------------|
| `component` | VARCHAR | Component type |
| `size_bytes` | BIGINT | Size in bytes |
| `percentage` | VARCHAR | Percentage of total file (e.g., "45.5%") |
| `block_count` | BIGINT | Number of blocks used |

**Components returned:**

| Component | Description |
|-----------|-------------|
| `table_data` | Column segment data across all tables |
| `index` | ART index blocks (won't shrink on DELETE) |
| `metadata` | Catalog, statistics, schema definitions |
| `free_blocks` | Blocks from deleted rows -- reusable but file won't shrink |
| `total` | Sum of all components (always 100.0%) |

## Example

```sql
-- Create a sample database
ATTACH 'demo.duckdb' AS demo;
USE demo;

CREATE TABLE events (
    event_id BIGINT,
    user_id INTEGER,
    session_id VARCHAR,
    event_type VARCHAR,
    event_data VARCHAR,
    timestamp TIMESTAMP,
    value DOUBLE
);

INSERT INTO events
SELECT
    i AS event_id,
    (random() * 10000)::INTEGER AS user_id,
    'session_' || (random() * 5000)::INTEGER AS session_id,
    (ARRAY['click', 'view', 'purchase', 'signup', 'logout'])[1 + (random() * 5)::INTEGER] AS event_type,
    'data_' || (random() * 1000)::INTEGER AS event_data,
    '2024-01-01 00:00:00'::TIMESTAMP + INTERVAL (i) SECOND AS timestamp,
    random() * 1000 AS value
FROM range(500000) t(i);

CHECKPOINT;
```

**Step 1: Check file sizes**
```sql
SELECT * FROM inspect_storage();
┌───────────────┬─────────────────────┬────────────────┐
│ database_name │ database_file_bytes │ wal_file_bytes │
│    varchar    │        int64        │     int64      │
├───────────────┼─────────────────────┼────────────────┤
│ demo          │             7602176 │              0 │
└───────────────┴─────────────────────┴────────────────┘
```

**Step 2: Per-table inspect**
```sql
SELECT * FROM inspect_database();
┌───────────────┬─────────────┬────────────┬──────────────────────┐
│ database_name │ schema_name │ table_name │ persisted_data_bytes │
│    varchar    │   varchar   │  varchar   │        int64         │
├───────────────┼─────────────┼────────────┼──────────────────────┤
│ demo          │ main        │ events     │              7340032 │
└───────────────┴─────────────┴────────────┴──────────────────────┘
```

**Step 3: Drill into a specific column**
```sql
SELECT * FROM inspect_column('events', 'event_id');
┌──────────────┬─────────────┬─────────────┬─────────────┬──────────────────┬──────────────────────────────┬───────────┐
│ row_group_id │ column_name │ column_type │ compression │ compressed_bytes │ estimated_decompressed_bytes │ row_count │
│    int64     │   varchar   │   varchar   │   varchar   │      int64       │            int64             │   int64   │
├──────────────┼─────────────┼─────────────┼─────────────┼──────────────────┼──────────────────────────────┼───────────┤
│            0 │ event_id    │ BIGINT      │ BitPacking  │             1208 │                       983040 │    122880 │
│            1 │ event_id    │ BIGINT      │ BitPacking  │             1208 │                       983040 │    122880 │
│            2 │ event_id    │ BIGINT      │ BitPacking  │             1208 │                       983040 │    122880 │
│            3 │ event_id    │ BIGINT      │ BitPacking  │             1208 │                       983040 │    122880 │
│            4 │ event_id    │ BIGINT      │ BitPacking  │              112 │                        67840 │      8480 │
└──────────────┴─────────────┴─────────────┴─────────────┴──────────────────┴──────────────────────────────┴───────────┘
```

**Step 4: Check storage breakdown**
```sql
SELECT * FROM inspect_block_usage();
┌─────────────┬────────────┬────────────┬─────────────┐
│  component  │ size_bytes │ percentage │ block_count │
│   varchar   │   int64    │  varchar   │    int64    │
├─────────────┼────────────┼────────────┼─────────────┤
│ table_data  │    7340032 │ 96.6%      │          28 │
│ index       │          0 │ 0.0%       │           0 │
│ metadata    │     262144 │ 3.4%       │           1 │
│ free_blocks │          0 │ 0.0%       │           0 │
│ total       │    7602176 │ 100.0%     │          29 │
└─────────────┴────────────┴────────────┴─────────────┘
```

## Roadmap

- [ ] Provide more information for per-table inspect (e.g., index)

## License

MIT License -- see [LICENSE](LICENSE) for details.

