from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

from store.service import Service

router = APIRouter(default_response_class=PlainTextResponse)
service = Service()


@router.get("/set")
def set_entry(name: str, value: int) -> str:
    """set a name and value entry

    Args:
        name (str): entry name
        value (int): entry value

    Returns:
        str: the name and value
    """
    service.set(name, value)
    return f"{name} = {value}"


@router.get("/get")
def get(name: str) -> str:
    """return the required entry

    Args:
        name (str): entry name

    Returns:
        str: entry name and value, if not present returns "None"
    """
    return str(service.get_entry(name))


@router.get("/unset")
def unset(name: str) -> str:
    """unsets the desired entry

    Args:
        name (str): entry name

    Returns:
        str: an indication that the entry was removed
    """
    service.unset(name)
    return f"{name} = None"


@router.get("/numequalto")
def numequalto(value: int) -> str:
    """return the count of entries with given value

    Args:
        value (int): requested value

    Returns:
        str: count value
    """
    return str(service.get_value_count(value))


@router.get("/undo")
def undo() -> str:
    """undo the most recent set/unset action

    Returns:
        str: the change or "NO COMMANDS" if there are no more commands to undo
    """
    return service.undo()


@router.get("/redo")
def redo() -> str:
    """redo any undone commands

    Returns:
        str: the change or "NO COMMANDS" if there are no more commands to redo
    """
    return service.redo()


@router.get("/end")
def end() -> str:
    """clears the db and restarts the service

    Returns:
        str: "CLEANED"
    """
    global service

    service.end()
    service = Service()
    return "CLEANED"
