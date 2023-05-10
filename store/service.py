from dataclasses import dataclass
from google.cloud import datastore

from store.scheme import Entry, GetResponse


@dataclass
class Command:
    pass


@dataclass
class NoCommand(Command):
    pass


@dataclass
class SetCommand(Command):
    name: str
    value: int


@dataclass
class UnsetCommand(Command):
    name: str
    value: int
    noop: bool


class Node:
    def __init__(self, command: Command) -> None:
        self.command: Command = command
        self.prev: Node | None = None
        self.next: Node | None = None


class Service:
    entry_entity = "Entry"
    value_count_entity = "ValueCount"

    def __init__(self) -> None:
        self.ds = datastore.Client()
        self.node: Node = Node(NoCommand())

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

    def _set_entry(self, name: str, value: int):
        with self.ds.transaction():
            key = self.ds.key(self.entry_entity, name)
            entry = self.ds.get(key=key)

            if entry:
                if entry["value"] == value:
                    return
                self._update_value_count(entry["value"], -1)

            entry = datastore.Entity(key=key)
            entry["value"] = value
            self.ds.put(entry)

            self._update_value_count(value, 1)

    def set(self, name: str, value: int):
        """upsert requested entity name

        Args:
            name (str): entity name
            value (int): new value
        """
        self._set_entry(name, value)

        self._append_node(Node(SetCommand(name, value)))

    def _unset_entry(self, name: str) -> tuple[int, bool]:
        with self.ds.transaction():
            key = self.ds.key(self.entry_entity, name)
            entry = self.ds.get(key=key)

            if entry:
                self._update_value_count(entry["value"], -1)
                self.ds.delete(key=key)
                return entry["value"], True

            return 0, False

    def unset(self, name: str):
        """unsets requested entity

        Args:
            name (str): entity name
        """
        res = self._unset_entry(name)
        self._append_node(Node(UnsetCommand(name, *res)))

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
        match self.node.command:
            case NoCommand():
                return "NO COMMANDS"
            case SetCommand(name, _):
                self._unset_entry(name)
                self.node = self.node.prev
                return f"{name} = None"
            case UnsetCommand(name, value, noop) if not noop:
                self._set_entry(name, value)
                self.node = self.node.prev
                return f"{name} = {value}"

        return "None"

    def redo(self) -> str:
        """redo last undone action

        Returns:
            str: indication
        """
        if self.node.next is None:
            return "NO COMMANDS"
        
        match self.node.next.command:
            case SetCommand(name, value):
                self._set_entry(name, value)
                self.node = self.node.next
                return f"{name} = {value}"
            case UnsetCommand(name, _, _):
                self._unset_entry(name)
                self.node = self.node.next
                return f"{name} = None"

        return "None"

    def end(self):
        """clear the db"""
        query = self.ds.query()
        query.keys_only()

        keys = (entity.key for entity in query.fetch())

        self.ds.delete_multi(keys)
