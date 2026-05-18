# Catalog MCP Module

Provides dataset discovery, registration, and management tools for SciDX.

## Capabilities
- Inspect library information and active context (`library_information`).
- Register datasets and consumption methods (`register_dataset`, `register_consumption_method`).
- Search for consumption methods across datasets (`search_consumption_methods`).
- Delete datasets from the catalogue (`delete_dataset`).

## Loading Only This Module
To start a BeeAI MCP session with just the catalog tools:

```python
streaming.configure_mcp(model="beeai-local", modules="catalog")
```

This ensures the agent prompt includes only catalog-related tools and avoids loading other modules.
