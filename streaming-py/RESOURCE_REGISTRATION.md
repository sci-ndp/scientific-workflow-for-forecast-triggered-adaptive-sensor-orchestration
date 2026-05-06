# Resource Registration Guide

This document describes how to register datasets and their consumption methods with the sciDX Streaming library.

## Dataset Metadata

Use `StreamingClient.register_data_source()` to create a CKAN dataset. Supply core CKAN fields inside `dataset_metadata`. The `notes` field is treated as the dataset description and displayed in CKAN.

```python
dataset_metadata = {
    "name": "air_quality_station",
    "title": "Air Quality Station",
    "notes": "Daily and real-time air quality observations",
    "owner_org": "enviro-lab",
}
```

## Consumption Method Payload

Each dataset resource is described by a payload with the following structure:

```python
{
    "type": "csv",              # required (csv|txt|json|stream|kafka|netcdf|rss)
    "name": "CSV daily export",  # required human-readable name
    "description": "Daily readings published as CSV",  # required
    "config": {...},              # required type-specific configuration
    "mapping": {...},             # optional field mappings shared across types
    "processing": {...},          # optional type-specific processing directives
}
```

### Common Optional Fields

- **mapping**: Dict defining column aliases or nested value extraction. Keys are desired output names; values support dotted paths and `[index]` notation (e.g. `"measurements[0]"`). When omitted, all source columns/keys are preserved.
- **processing**: Dict containing additional type-specific hints (e.g., CSV delimiter, JSON data key). Each type’s supported options are listed below.

## Type Reference

### CSV / TXT

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `url` | config | ✓ | HTTP(S) location of the text or CSV file. |
| `delimiter` | processing | optional | Field separator (defaults: `\t` for TXT, autodetect for CSV). |
| `header_line` | processing | optional | Zero-based line index containing column headers (default: 0). |
| `start_line` | processing | optional | First data line (default: 1 when headers present). |

### JSON (batch)

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `url` | config | ✓ | HTTP(S) endpoint returning JSON payloads. |
| `data_key` | processing | optional | Dotted path pointing to the list or dict containing records. |
| `info_key` | processing | optional | Path whose values should be merged into `stream_info`. |
| `additional_key` | processing | optional | Path whose content is appended to `stream_info['additional_info']`. |

### STREAM (HTTP streaming / SSE)

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `url` | config | ✓ | Streaming endpoint (SSE or JSON lines). |
| `data_key` | processing | optional | Nested key to extract records before mapping. |
| `batch_mode` | config | optional | `'True'` or `'False'` string to treat messages as columnar batches. |
| `time_window` | config | optional | Seconds to wait before flushing when idle (default 10). |
| `batch_interval` | config | optional | Minimum seconds between flushes (default 1). |

### Kafka

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `host` | config | ✓ | Kafka bootstrap host. |
| `port` | config | ✓ | Kafka bootstrap port. |
| `topic` | config | ✓ | Source topic name. |
| `security_protocol` | config | optional | e.g., `SASL_SSL`, `SSL`. |
| `sasl_mechanism` | config | optional | e.g., `PLAIN`. Requires credentials in POP secrets. |
| `auto_offset_reset` | config | optional | `earliest` (default) or `latest`. |
| `time_window` | config | optional | Seconds to wait for new data before logging inactivity (default 10). |
| `batch_interval` | config | optional | Minimum seconds between flushes (default 1). |
| `data_key` | processing | optional | Nested key to extract values from decoded messages. |

### NetCDF

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `url` | config | ✓ | HTTP(S) URL to the NetCDF (.nc/.nc4) file. |
| `group` | config | optional | HDF5 group within the NetCDF file. |

### RSS

| Field | Location | Required | Description |
|-------|----------|----------|-------------|
| `url` | config | ✓ | RSS/Atom feed URL. |
| `fetch_mode` | processing | optional | `continuous` (default) or `once`. |
| `poll_interval` | processing | optional | Seconds between feed polls (default 30). |
| `duration` | processing | optional | Total seconds to poll before stopping (default 86400). |

## Examples

### CSV with Mapping and Processing
```python
{
    "type": "csv",
    "name": "CSV daily export",
    "description": "Daily readings published as CSV",
    "config": {"url": "https://data.example.org/air/measurements.csv"},
    "mapping": {"ozone": "O3", "timestamp": "timestamp"},
    "processing": {"delimiter": ",", "header_line": 0, "start_line": 1},
}
```

### Kafka Stream
```python
{
    "type": "kafka",
    "name": "Real-time Kafka feed",
    "description": "Live events produced by the station",
    "config": {
        "host": "broker.example.org",
        "port": 9092,
        "topic": "air.quality.station1",
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "auto_offset_reset": "earliest",
        "time_window": 10,
        "batch_interval": 1,
    },
    "processing": {"data_key": "payload.records"},
}
```

### RSS Feed
```python
{
    "type": "rss",
    "name": "Station alert feed",
    "description": "Syndicated alerts for the station",
    "config": {"url": "https://data.example.org/air/alerts.rss"},
    "processing": {"fetch_mode": "continuous", "poll_interval": 60},
}
```

## Tips

- **Mapping** works for every type; omit it to forward all available fields.
- **Processing** sections are optional, but when provided they must be dictionaries. Unsupported keys are ignored gracefully.
- Re-running `register_data_source` with the same dataset slug is idempotent; CKAN will update existing resources when methods share names.
- Use `StreamingClient.search_consumption_methods()` to confirm registered methods before creating derived streams.
