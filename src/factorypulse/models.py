from datetime import datetime
from pydantic import BaseModel

class SensorReading(BaseModel):
    """"
    Represents a single read of a sensor
    Each sensor measures only one magnitude (Ex.: Temperature, Vibration, Energy Consumption...)
    of a specific machine in a sector. 
    """
    sensor_id: str
    machine_id: str
    sector: str
    sensor_type: str
    value: float
    unit: str
    timestampe: datetime