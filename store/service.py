from dataclasses import dataclass
from google.cloud import datastore

from store.scheme import Entry, GetResponse


@dataclass
class State:
    pass


@dataclass
class Noop(State):
    pass


@dataclass
class Transition(State):
    name: str
    old: str
    new: str


class Node:
    def __init__(self, state: State) -> None:
        self.state: State = state
        self.prev: Node | None = None
        self.next: Node | None = None


class Service:
    entry_entity = "Entry"
    value_count_entity = "ValueCount"

    def __init__(self) -> None:
        self.ds = datastore.Client()
        self.node: Node = Node(Noop())

    def _append_node(self, node: Node):
        node.prev = self.node
        self.node.next = node
        self.node = self.node.next

    def _update_value_count(self, value: int, diff: int):
        key = self.ds.key(self.value_count_entity, value)
        value_count = self.ds.get(key=key)

        if value_count is None:
            value_count = datastore.Entity(key=key)
            value_count["count"] = 0

        value_count["count"] += diff
        self.ds.put(value_count)

    def _set_entry(self, name: str, value: str) -> State:
        with self.ds.transaction():
            key = self.ds.key(self.entry_entity, name)
            entry = self.ds.get(key=key)

            old_value = "None"
            if entry:
                if entry["value"] == value:
                    return Transition(name=name, old=value, new=value)
                if entry["value"] != "None":
                    self._update_value_count(int(entry["value"]), -1)
                old_value = entry["value"]

            entry = datastore.Entity(key=key)
            entry["value"] = value
            self.ds.put(entry)

            if value != "None":
                self._update_value_count(int(value), 1)
            return Transition(name=name, old=old_value, new=value)

    def set(self, name: str, value: int):
        """upsert requested entity name

        Args:
            name (str): entity name
            value (int): new value
        """
        transition = self._set_entry(name, str(value))
        self._append_node(Node(state=transition))

    def unset(self, name: str):
        """unsets requested entity

        Args:
            name (str): entity name
        """
        transition = self._set_entry(name, "None")
        self._append_node(Node(transition))

    def get_entry(self, name: str) -> GetResponse:
        """returns desired entry

        Args:
            name (str): entry name

        Returns:
            GetResponse: entry wrapper to encapsulate cases where there isn't anything to return
        """
        key = self.ds.key(self.entry_entity, name)
        entity = self.ds.get(key=key)

        return GetResponse(
            entry=Entry(name=name, value=entity["value"]) if entity != None else None
        )

    def get_value_count(self, value: int) -> int:
        """return the count of entries with given value

        Args:
            value (int): requested value

        Returns:
            int: count value
        """
        key = self.ds.key(self.value_count_entity, value)
        value_count = self.ds.get(key=key)

        return value_count["count"] if value_count else 0

    def undo(self) -> str:
        """undo last set/unset action

        Returns:
            str: indication
        """
        match self.node.state:
            case Noop():
                return "NO COMMANDS"
            case Transition(name, old, _):
                self._set_entry(name, old)
                self.node = self.node.prev
                return f"{name} = {old}"

        return "None"

    def redo(self) -> str:
        """redo last undone action

        Returns:
            str: indication
        """
        if self.node.next is None:
            return "NO COMMANDS"

        match self.node.next.state:
            case Transition(name, _, new):
                self._set_entry(name, new)
                self.node = self.node.next
                return f"{name} = {new}"

        return "None"

    def end(self):
        """clear the db"""
        query = self.ds.query()
        query.keys_only()

        keys = (entity.key for entity in query.fetch())

        self.ds.delete_multi(keys)
