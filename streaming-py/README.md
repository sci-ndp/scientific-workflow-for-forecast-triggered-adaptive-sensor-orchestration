# scidx Streaming

A Python library for managing streaming data using the sciDX platform and a Point of Presence. This library provides easy-to-use methods for creating, consuming, and managing Kafka streams and related resources.


## Table of Contents

- [Installation](https://github.com/sci-ndp/streaming-py/blob/main/README.md#installation)
- [Tutorial](https://github.com/sci-ndp/streaming-py/blob/main/README.md#tutorial)
- [Running Tests](https://github.com/sci-ndp/streaming-py/blob/main/README.md#running-tests)
- [Usage examples](https://github.com/sci-ndp/streaming-py/blob/main/README.md#usage-examples)
- [Contributing](https://github.com/sci-ndp/streaming-py/blob/main/README.md#contributing)
- [License](https://github.com/sci-ndp/streaming-py/blob/main/README.md#license)
- [Contact](https://github.com/sci-ndp/streaming-py/blob/main/README.md#contact)


## Installation

Ensure you have Python 3.7 or higher installed. Using a virtual environment is recommended.

### Option 1: Install from GitHub

1. **Clone the repository:**

   ```bash
   git clone https://github.com/sci-ndp/streaming-py.git
   cd streaming-py
   ```
2. **Create and activate a virtual environment:**

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
3. **Install the package in editable mode:**

   ```bash
   pip install -e .
   ```
4. **Install development dependencies (optional, for testing):**

   ```bash
   pip install -r requirements.txt
   ```

### Snappy codec support

Kafka streams provided by sciDX are Snappy-compressed. The `python-snappy` wheel is pulled in automatically by the library, but it still needs the native `libsnappy` runtime on your machine. Install it before consuming streams:

- **Ubuntu/Debian**: `sudo apt-get install libsnappy-dev`
- **macOS (Homebrew)**: `brew install snappy`
- **Windows (Chocolatey)**: `choco install snappy`

If you encounter `UnsupportedCodecError: Libraries for snappy compression codec not found`, install the native dependency and reinstall `python-snappy` in your virtual environment.

### Option 2: Install via pip

Once the package is published on PyPI, you can install it directly using pip:

```
pip install scidx-streaming
```

## Tutorial

For a step-by-step guide on how to use the `streaming` library, check out our comprehensive tutorial: [10 Minutes for Streaming POP Data](https://github.com/sci-ndp/streaming-py/blob/main/docs/streaming_tutorial.ipynb).

Looking for the full architecture map? Read [`docs/STREAMING_LIBRARY_README.md`](docs/STREAMING_LIBRARY_README.md); it is the canonical reference for every module, MCP tool, and companion notebook.


## Running Tests

To run the tests, navigate to the project root and execute:

```bash
pytest
```

## Usage examples

Below is an example showcasing how to set up the library, register a data object, create a filtered stream, and consume its data.

### 1. Set up the POP and Streaming libraries

This can be done by initializing the `APIClient` and using it to initilize the `StreamingClient`:

```python
from streaming import StreamingClient
from ndp_ep import APIClient

API_URL = "http://your-api-url.com"
USERNAME = "your_username"
PASSWORD = "your_password"

client = APIClient(base_url=API_URL, username=USERNAME, password=PASSWORD)
streaming = StreamingClient(client)
```

### 2. Register a data object

```python
data_object_metadata = {
    "name": "sample_data_object",
    "type": "url",
    "url": "http://example.com/data.csv",
    "description": "Sample data object for streaming demo"
}
client.register_url(data_object_metadata)
```

### 3. Create a filtered data stream

```python
# Discover any existing consumption methods
datasets = streaming.search_consumption_methods(["sample_data_object"])
resource_ids = [res["id"] for ds in datasets for res in ds["resources"]]

# Define filters
filters = [
    "column_name > 100",
    "IF column_name < 50 THEN alert = 'low' ELSE alert = 'high'",
]

# Create a Kafka stream with filters
producer = await streaming.create_kafka_stream(
    consumption_method_ids=resource_ids,
    filter_semantics=filters,
)
print(f"Stream created with topic: {producer.data_stream_id}")
```

### 4. Consuming the filtered data stream

```python
# Consume stream data
consumer = streaming.consume_kafka_messages(producer.data_stream_id)
print(consumer.dataframe.head())
```

### 5. Cleaning up

```python
consumer.stop()
# Delete the stream and the data object
await streaming.delete_stream(producer)
client.delete_resource_by_id(resource_ids[0])
print("Cleanup completed.")
```

## BeeAI MCP Architecture

The `agentic_mcp` package turns the streaming helpers into BeeAI tools that can be orchestrated locally or exposed through the Model Context Protocol. For the full module inventory, profile system, and remote-module guidance, read:

- [`docs/STREAMING_LIBRARY_README.md`](docs/STREAMING_LIBRARY_README.md) – canonical reference with per-module file listings.
- [`docs/agentic_mcp_guide.md`](docs/agentic_mcp_guide.md) – hands-on instructions for configuring BeeAI profiles, remote MCP modules, and specialised setups (EarthScope, diagnostics-only agents, etc.).

Those guides stay in sync with the codebase and include curated examples for the scenarios summarized above.
## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the repository**
2. **Create a new branch** (`git checkout -b feature/new-feature`)
3. **Make your changes** and **commit** (`git commit -m 'Add new feature'`)
4. **Push** to the branch (`git push origin feature/new-feature`)
5. **Open a Pull Request**

### Contributing in PyPI

To publish the library to PyPI, follow these steps:

Ensure setup.py is correctly configured.

Build the distribution files:

```bash
python setup.py sdist bdist_wheel
```

Upload to PyPI using twine:

```bash
twine upload dist/*
```

Verify the package on PyPI:

Visit https://pypi.org/ and check your package listing.

If you need to update the library on PyPI:
- Make your changes and update the version in setup.py.
- Run the above steps to rebuild and upload the new version.

## License

This project is licensed under the MIT License. See [LICENSE.md](https://github.com/sci-ndp/streaming-py/blob/main/docs/LICENSE.md) for more details.

## Contact

For any questions or suggestions, please open an [issue](https://github.com/sci-ndp/streaming-py/blob/main/docs/issues.md) on GitHub.
