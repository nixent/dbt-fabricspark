from typing import Any
from enum import EnumMeta, Enum


class CaseInsensitiveEnumMeta(EnumMeta):
    """Enum metaclass to allow for interoperability with case-insensitive strings.
    """

    def __getitem__(cls, name: str) -> Any:
        # disabling pylint bc of pylint bug https://github.com/PyCQA/astroid/issues/713
        return super(CaseInsensitiveEnumMeta, cls).__getitem__(name.upper())  # pylint: disable=no-value-for-parameter

    def __getattr__(cls, name: str) -> Enum:
        """Return the enum member matching `name`.
        """
        try:
            return cls._member_map_[name.upper()]
        except KeyError as err:
            raise AttributeError(name) from err
        
class LivyStatementStates(str, Enum, metaclass=CaseInsensitiveEnumMeta):
    """Livy statement state
    """
    WAITING = "waiting"
    RUNNING = "running"
    AVAILABLE = "available"
    ERROR = "error"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"

class LivyStates(str, Enum, metaclass=CaseInsensitiveEnumMeta):
    """Livy session state
    """

    NOT_STARTED = "not_started"
    STARTING = "starting"
    IDLE = "idle"
    BUSY = "busy"
    SHUTTING_DOWN = "shutting_down"
    ERROR = "error"
    DEAD = "dead"
    KILLED = "killed"
    SUCCESS = "success"
    RUNNING = "running"
    RECOVERING = "recovering"