# Insights MCP Module

Provides diagnostics and service discovery helpers for the MCP stack.

## Capabilities
- Describe registered services and capabilities (`describe_services`).
- Inspect LLM diagnostics (`llm_diagnostics`).
- List bundled LLM profiles (`list_llm_profiles`).

## Loading Only This Module
```python
streaming.configure_mcp(model="beeai-local", modules="insights")
```
