from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file = ".env", extra = "ignore")
    kafka_bootstrap_servers: str = "kafka-1:9092,kafka-2:9092"
    kafka_topic: str = "dados-sensores"

    sensor_id: str
    machine_id: str
    sector: str
    sensor_type: Literal[
        "temperature", "vibration", "energy_consumption", "pressure", "photovoltaic"
    ]
    
    send_interval_seconds: float = 2.0
    anomaly_probability: float = 0.05

    @property
    def kafka_bootstrap_servers_list(self) -> list[str]:
        return self.kafka_bootstrap_servers.split(",")


settings = Settings()