from pydantic import BaseModel



class Intel(BaseModel):
    timestamp: str
    signal_id: str
    entity_id: str
    reported_lat: float
    reported_lon: float
    signal_type: str
    priority_level: int


class Attack(BaseModel):
    timestamp: str
    attack_id: str
    entity_id: str
    weapon_type: float


class Damage(BaseModel):
    timestamp: str
    attack_id: str
    entity_id: str
    result: str