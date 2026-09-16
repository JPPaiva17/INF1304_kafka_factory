import json
import time
from datetime import datetime, timezone
import random

from kafka import KafkaProducer

from src.factorypulse.config import settings
from src.factorypulse.models import SensorReading

NORMAL_RANGES = {
    "temperature": {
        "min": 20.0,
        "max": 75.0,
        "anomaly_max": 120.0,
        "unit": "celsius",
    },
    "vibration": {
        "min": 0.1,
        "max": 4.0,
        "anomaly_max": 12.0,
        "unit": "mm/s",
    },
    "energy_consumption": {
        "min": 5.0,
        "max": 40.0,
        "anomaly_max": 90.0,
        "unit": "kwh",
    },
    "pressure": {
        "min": 1.0,
        "max": 8.0,
        "anomaly_max": 15.0,
        "unit": "bar",
    },
    "photovoltaic": {
        "min": 0.0,
        "max": 350.0,
        "anomaly_max": 500.0,
        "unit": "watts",
    },
}


class SensorProducer:
    """
    Represents a simulated physical sensor publishing readings to Kafka.

    Each instance corresponds to a single sensor, identified by the
    configuration read from Settings (injected via environment variables).
    """

    def __init__(self):
        self.producer = self._create_kafka_producer()

    def _create_kafka_producer(self) -> KafkaProducer:
        """Creates and configures the KafkaProducer client."""
        return KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers_list,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
        )

    def _generate_reading(self) -> SensorReading:
        """Generates a simulated reading, with a chance of anomaly."""
        value_range = NORMAL_RANGES[settings.sensor_type]
        is_anomaly = random.random() < settings.anomaly_probability

        if is_anomaly:
            value = round(random.uniform(value_range["max"], value_range["anomaly_max"]), 2)
        else:
            value = round(random.uniform(value_range["min"], value_range["max"]), 2)

        return SensorReading(
            sensor_id=settings.sensor_id,
            machine_id=settings.machine_id,
            sector=settings.sector,
            sensor_type=settings.sensor_type,
            value=value,
            unit=value_range["unit"],
            timestamp=datetime.now(timezone.utc),
        )

    def send_reading(self) -> SensorReading:
        """Generates and publishes a single reading to the Kafka topic."""
        reading = self._generate_reading()
        self.producer.send(
            settings.kafka_topic,
            key=settings.machine_id,
            value=reading.model_dump(mode="json"),
        )
        return reading

    def run(self):
        """Main loop: sends readings periodically until interrupted."""
        print(
            f"[{settings.sensor_id}] starting to send to "
            f"'{settings.kafka_topic}' every {settings.send_interval_seconds}s"
        )
        try:
            while True:
                reading = self.send_reading()
                print(f"[{settings.sensor_id}] sent: {reading.model_dump_json()}")
                time.sleep(settings.send_interval_seconds)
        except KeyboardInterrupt:
            print(f"[{settings.sensor_id}] shutting down...")
        finally:
            self.close()

    def close(self):
        """Ensures pending messages are flushed before closing."""
        self.producer.flush()
        self.producer.close()


if __name__ == "__main__":
    sensor = SensorProducer()
    sensor.run()