from typing import Dict, List, Optional

from pydantic import BaseModel


class DayData(BaseModel):
    date: str
    hours_start: Optional[int] = None
    hours_end: Optional[int] = None
    hours_count: int
    temp_avg: Optional[float] = None
    relevant_cond_hours: int


class CityData(BaseModel):
    days: List[DayData]


class WeatherData(BaseModel):
    cities: Dict[str, CityData]
