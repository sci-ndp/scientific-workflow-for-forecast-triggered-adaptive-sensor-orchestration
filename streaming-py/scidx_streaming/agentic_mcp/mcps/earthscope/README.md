# EarthScope MCP Module

Tailored toolkit for the EarthScope demonstration, shipping with pre-configured helpers.

## Capabilities
- `start_earthscope_stream`: boots a stream using the configured consumption method IDs and credentials.
- `build_station_filter`: searches for station resources in a given area and emits `station IN [...]` semantics.
- `launch_earthscope_visualization`: opens a Matplotlib snapshot (or returns a JSON sample when plotting is unavailable).

Defaults such as resource IDs and credentials are defined in `config.yaml` under `extras`.

## Loading Only This Module
```python
streaming.configure_mcp(model="beeai-local", modules="earthscope")
```
