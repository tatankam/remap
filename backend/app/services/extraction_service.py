#import json
from typing import Optional, Literal
from pydantic import BaseModel, ValidationError, model_validator, Field, field_validator
from datetime import datetime, timedelta
from crewai import Agent, Task, Crew, Process, LLM
from app.core.config import OPENAI_API_KEY, OPEN_AI_BASE_URL, OPENAI_MODEL


customllm = LLM(
    model=OPENAI_MODEL,
    base_url=OPEN_AI_BASE_URL,
    api_key=OPENAI_API_KEY,
    temperature=0.0,
)


ProfileChoice = Literal["driving-car", "cycling-regular", "foot-walking"]


def default_start_date():
    return datetime.now().isoformat()


def default_end_date():
    return (datetime.now() + timedelta(days=4)).isoformat()


class Payload(BaseModel):
    origin_address: str
    destination_address: str
    buffer_distance: Optional[float] = 5.0
    startinputdate: Optional[str] = Field(default_factory=default_start_date)
    endinputdate: Optional[str] = Field(default_factory=default_end_date)
    query_text: Optional[str] = ""
    numevents: Optional[int] = 10
    profile_choice: Optional[ProfileChoice] = "driving-car"

    @field_validator('buffer_distance', mode='before')
    def set_buffer_distance_default(cls, v):
        if v is None:
            return 5.0
        return v

    @field_validator('numevents', mode='before')
    def set_numevents_default(cls, v):
        if v is None:
            return 10
        return v

    @model_validator(mode="after")
    def check_date_order(cls, model):
        start = datetime.fromisoformat(model.startinputdate)
        end = datetime.fromisoformat(model.endinputdate)
        if start > end:
            raise ValueError("start date can't be later than end date")
        return model


agent = Agent(
    role="Payload Extractor",
    goal=(
        "Given an input sentence, extract ONLY the following fields as JSON: "
        "origin_address, destination_address, buffer_distance (in km), startinputdate (ISO 8601 date-time string for departure), "
        "endinputdate (ISO 8601 date-time string for arrival), query_text (search keywords found after phrases like 'about', 'on', or 'for', else default ''), "
        "numevents (integer), profile_choice (one of 'driving-car', 'cycling-regular', 'foot-walking'; default 'driving-car'). "
        "You must parse these fields dynamically from the input sentence provided via 'input' variable. "
        "Do not return default or example values unless they appear explicitly in the input sentence. "
        "Output ONLY the JSON object, no additional commentary."
    ),
    backstory="Expert at precise structured extraction from unstructured text sentences.",
    tools=[],
    llm=customllm,
    verbose=True,
    allow_delegation=False,
)


task = Task(
    description=(
        "Extract the payload data from this input sentence dynamically:\n"
        "{input}\n\n"
        "Return ONLY a JSON object matching the following format (with profile_choice restricted to specific values):\n"
        '{\n'
        '  "origin_address": "Padova",\n'
        '  "destination_address": "Venice",\n'
        '  "buffer_distance": 6.0,\n'
        '  "startinputdate": "2025-09-03T06:00:00",\n'
        '  "endinputdate": "2025-09-07T15:00:00",\n'
        '  "query_text": "",\n'  # empty string default here
        '  "numevents": 13,\n'
        '  "profile_choice": "driving-car"\n'
        '}\n'
        "Use the values from the input sentence above, not the example values here. Extract query_text from phrases like 'about music', 'on theater', 'for workshop', etc. If no such keywords found, set query_text to an empty string."
    ),
    expected_output="A JSON object matching the Payload pydantic model with profile_choice and dynamic query_text.",
    agent=agent,
    output_json=Payload,
)


crew = Crew(
    agents=[agent],
    tasks=[task],
    verbose=True,
    process=Process.sequential,
)


def extract_payload(sentence: str):
    result = crew.kickoff(inputs={"input": sentence})
    try:
        payload = Payload.model_validate(result.to_dict())
        return payload
    except ValidationError as e:
        print("Validation failed:", e)
        return None
