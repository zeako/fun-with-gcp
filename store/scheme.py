from pydantic import BaseModel


class Entry(BaseModel):
    name: str
    value: int

    def __str__(self) -> str:
        return f"{self.value}"


class GetResponse(BaseModel):
    entry: Entry | None = None

    def __str__(self) -> str:
        return self.entry.__str__() if self.entry != None else "None"
