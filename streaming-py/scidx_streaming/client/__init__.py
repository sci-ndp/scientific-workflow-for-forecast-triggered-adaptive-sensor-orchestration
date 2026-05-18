from .init_client import StreamingClient
from .create_kafka_stream import create_kafka_stream
from .consume_kafka_messages import consume_kafka_messages
from .delete_stream import delete_stream
from .discover_stream import my_streams, public_streams
# Add the methods to StreamingClient
StreamingClient.create_kafka_stream = create_kafka_stream
StreamingClient.consume_kafka_messages = consume_kafka_messages
StreamingClient.delete_stream = delete_stream
StreamingClient.my_streams = my_streams
StreamingClient.public_streams = public_streams
