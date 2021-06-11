from dataclasses import dataclass
from datetime import datetime

@dataclass
class Sample:
    machine_name: str
    time: datetime

class WOSSample(Sample):
    ENGRPM: float
    ENGCOOLT: float
    ENGOILP: float
    ENGHOURS: float
    FUELUS: float
    INTAKEP: float
    INTAKET: float
    TRNLUP: float
    GROILP: float
    GROILP: float
    GROILT: float
    SELGEAR: float
    SPEED: float
    BREAKP: float
    HYDDRV: float
    TRNAUT: float
    HYDOILT: float
    HYDOILP: float
    TEMPIN: float
    ENGRPM: float
    ENGCOOLT: float
    ENGOILP: float
    ENGHOURS: float
    FUELUS: float
    INTAKEP: float
    INTAKET: float
    TRNLUP: float
    GROILP: float
    GROILT: float
    SELGEAR: float
    SPEED: float
    BREAKP: float
    HYDDRV: float
    TRNAUT: float
    HYDOILT: float
    HYDOILP: float
    TEMPIN: float
