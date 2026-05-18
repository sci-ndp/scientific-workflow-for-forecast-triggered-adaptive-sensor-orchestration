# Filtering MCP Module

Offers tools for consuming and visualising Kafka streams.

## Capabilities
- Begin consuming a stream (`consume_stream`).
- Preview buffered data (`visualize_stream`).
- Stop consumers (`stop_consumer`).
- Delete streams (`delete_stream`).

## Loading Only This Module
```python
streaming.configure_mcp(model="beeai-local", modules="filtering")
```
