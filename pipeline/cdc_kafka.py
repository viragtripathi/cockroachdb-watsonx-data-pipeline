"""
Kafka CDC Consumer for Debezium CockroachDB connector.

Consumes Debezium change events from Kafka topics and feeds them
to the common CDC processor for batching and Parquet writing.

Requires: pip install cockroachdb-watsonx-data-pipeline[kafka]
"""

import json
import signal
import threading

from .config import PipelineConfig
from .processor import CDCProcessor


class KafkaConsumer:
    """Consumes Debezium CDC events from Kafka and feeds them to the processor."""

    def __init__(self, config: PipelineConfig, processor: CDCProcessor):
        self.config = config
        self.processor = processor
        self._running = False
        self._consumer = None
        self._thread: threading.Thread | None = None

    def _create_consumer(self):
        from confluent_kafka import Consumer

        return Consumer({
            "bootstrap.servers": self.config.kafka_bootstrap_servers,
            "group.id": self.config.kafka_group_id,
            "auto.offset.reset": self.config.kafka_auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
        })

    def start(self, background: bool = True) -> None:
        """Start consuming Kafka messages."""
        self._consumer = self._create_consumer()
        self._consumer.subscribe(self.config.kafka_topics)
        self._running = True

        print("🔌 Kafka consumer started")
        print(f"   Brokers: {self.config.kafka_bootstrap_servers}")
        print(f"   Topics: {self.config.kafka_topics}")
        print(f"   Group: {self.config.kafka_group_id}")

        if background:
            self._thread = threading.Thread(target=self._consume_loop, daemon=True)
            self._thread.start()
        else:
            self._consume_loop()

    def _consume_loop(self) -> None:
        """Main consume loop."""
        try:
            while self._running:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"⚠️  Kafka error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    self.processor.process_event(value)
                except json.JSONDecodeError as e:
                    print(f"⚠️  Invalid JSON from Kafka: {e}")
                except Exception as e:
                    print(f"⚠️  Error processing Kafka message: {e}")
        finally:
            if self._consumer:
                self._consumer.close()
                print("🔌 Kafka consumer stopped")

    def stop(self) -> None:
        """Stop consuming."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=10)


def run_kafka_consumer(config: PipelineConfig, processor: CDCProcessor) -> KafkaConsumer:
    """Create and start a Kafka consumer. Returns the consumer for lifecycle management."""
    consumer = KafkaConsumer(config, processor)

    def signal_handler(sig, frame):
        print("\nStopping Kafka consumer...")
        consumer.stop()
        processor.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer.start(background=False)
    return consumer
