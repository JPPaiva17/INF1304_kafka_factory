from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file = ".env", extra = "ignore")
    kafka_bootstrap_servers: str = "kafka-1:9092,kafka-2:9092"
    kafka_topic: str = "dados-sensores"

    sensor_id: str
    machine_id: str
    setor: str
    sensor_type: str
    
    intervalo_envio_segundos: float = 2.0
    probabilidade_anomalia: float = 0.05

    @property
    def kafka_bootstrap_servers_list(self) -> list[str]:
        return self.kafka_bootstrap_servers.split(",")


settings = Settings()