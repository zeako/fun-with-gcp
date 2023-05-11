from dataclasses import dataclass
from google.cloud import datastore

from store.scheme import Entry


@dataclass
class State:
    pass


@dataclass
class Noop(State):
    pass


@dataclass
class Transition(State):
    name: str
    old: str = "None"
    new: str = "None"


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

    def _update_value_count(self, value: str, n: int):
        key = self.ds.key(self.value_count_entity, value)
        value_count = self.ds.get(key=key)

        if value_count is None:
            value_count = datastore.Entity(key=key)
            value_count["count"] = 0

        value_count["count"] += n
        self.ds.put(value_count)

    def _set_entry(self, entry: Entry) -> State:
        with self.ds.transaction():
            key = self.ds.key(self.entry_entity, entry.name)
            entity = self.ds.get(key=key)

            transition = Transition(name=entry.name, new=entry.value)

            if entity:
                entity_val = entity["value"]

                # early exit
                if entity_val == entry.value:
                    transition.old = entry.value
                    return transition

                if entity_val != "None":
                    self._update_value_count(entity_val, -1)
                transition.old = entity_val

            entity = datastore.Entity(key=key)
            entity["value"] = entry.value
            self.ds.put(entity)

            if entry.value != "None":
                self._update_value_count(entry.value, 1)
            return transition

    def set(self, entry: Entry):
        """upsert requested entity name

        Args:
            entry (Entry): desired entry to set
        """
        transition = self._set_entry(entry)
        self._append_node(Node(state=transition))

    def unset(self, entry: Entry):
        """unsets requested entity

        Args:
            entry (Entry): desired entry to unset
        """
        transition = self._set_entry(entry)
        self._append_node(Node(transition))

    def get_entry(self, name: str) -> Entry:
        """returns desired entry

        Args:
            name (str): entry name

        Returns:
            GetResponse: entry wrapper to encapsulate cases where there isn't anything to return
        """
        key = self.ds.key(self.entry_entity, name)
        entity = self.ds.get(key=key)

        entry = Entry(name=name)
        if entity is not None:
            entry.value = entity["value"]
        return entry

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

    def undo(self) -> Entry | None:
        """undo last set/unset action

        Returns:
            Entry | None: entity object or None in case of noop
        """
        match self.node.state:
            case Noop():
                return None
            case Transition(name, old, _):
                entry = Entry(name=name, value=old)
                self._set_entry(entry)
                self.node = self.node.prev
                return entry

    def redo(self) -> Entry | None:
        """redo last undone action

        Returns:
            Entry | None: entity object or None in case of noop
        """
        if self.node.next is None:
            return None

        match self.node.next.state:
            case Transition(name, _, new):
                entry = Entry(name=name, value=new)
                self._set_entry(entry)
                self.node = self.node.next
                return entry

    def end(self):
        """clear the db"""
        query = self.ds.query()
        query.keys_only()

        keys = (entity.key for entity in query.fetch())

        self.ds.delete_multi(keys)
