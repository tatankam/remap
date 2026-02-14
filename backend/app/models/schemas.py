from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import Optional, Literal

ProfileChoice = Literal["driving-car", "cycling-regular", "foot-walking"]

class RouteRequest(BaseModel):
    origin_address: str = Field(..., example="Padova", description="Starting location (ALWAYS required)")
    destination_address: Optional[str] = Field(None, example="Verona", description="Ending location (required for route mode, omit for point mode)")
    buffer_distance: float = Field(..., gt=0, example=5.0, description="Buffer distance in KM around route/point (ALWAYS required, must be >0)")
    startinputdate: datetime = Field(..., example="2025-08-23T13:28:39Z", description="Start date for event filtering")
    endinputdate: datetime = Field(..., example="2025-08-27T13:28:39Z", description="End date for event filtering")
    query_text: Optional[str] = Field(default="", example="Music", description="Semantic search text (empty = no text filtering)")
    numevents: Optional[int] = Field(default=100, example=100, description="Maximum number of events to retrieve")
    profile_choice: Optional[ProfileChoice] = Field(default="driving-car", example="cycling-regular", description="Routing profile: 'driving-car', 'cycling-regular', 'foot-walking'")

    @model_validator(mode='after')
    def validate_mode_logic(self):
        """Ensure valid point OR route mode"""
        if self.destination_address is None:
            # Point mode: origin + buffer only
            if self.buffer_distance <= 0:
                raise ValueError("buffer_distance must be >0 for point mode (no destination)")
        else:
            # Route mode: origin + destination + buffer
            if self.buffer_distance <= 0:
                raise ValueError("buffer_distance must be >0 for route mode")
        
        # Validate dates
        if self.startinputdate >= self.endinputdate:
            raise ValueError("startinputdate must be before endinputdate")
        
        return self


class SentenceInput(BaseModel):
    sentence: str = Field(
        ..., 
        example="I want to go from Vicenza to Trento and I will leave 14 September 2025 at 2 a.m. and I will arrive on 11 October at 5:00. Give me 11 events about workshop in a range of 6 km. Use cycling-regular transport.",
        description="Natural language sentence to parse into RouteRequest parameters"
    )
