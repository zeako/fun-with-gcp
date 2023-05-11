from pydantic import BaseModel


class Entry(BaseModel):
    name: str
    value: str = "None"
