from dataclasses import dataclass
from datetime import datetime


@dataclass
class Sample:
    machine_name: str
    time: datetime


@dataclass
class WOSSample(Sample):
    ENGRPM: float = None
    ENGCOOLT: float = None
    ENGOILP: float = None
    ENGHOURS: float = None
    ENGTPS: float = None
    FUELUS: float = None
    INTAKEP: float = None
    INTAKET: float = None
    TRNLUP: float = None
    GROILP: float = None
    GROILP: float = None
    GROILT: float = None
    SELGEAR: float = None
    SPEED: float = None
    BREAKP: float = None
    HYDDRV: float = None
    TRNAUT: float = None
    HYDOILT: float = None
    HYDOILP: float = None
    TEMPIN: float = None
    ENGRPM: float = None
    ENGCOOLT: float = None
    ENGOILP: float = None
    ENGHOURS: float = None
    FUELUS: float = None
    INTAKEP: float = None
    INTAKET: float = None
    TRNLUP: float = None
    GROILP: float = None
    GROILT: float = None
    SELGEAR: float = None
    SPEED: float = None
    BREAKP: float = None
    HYDDRV: float = None
    TRNAUT: float = None
    TRNBPS: float = None
    HYDOILT: float = None
    HYDOILP: float = None
    TEMPIN: float = None
