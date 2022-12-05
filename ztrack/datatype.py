from dataclasses import dataclass
import dataclasses
import json
from typing import Any, Callable, Dict, List, Literal, Optional, Union


@dataclass
class ArtifactDataType:
    _type_: Literal["artifact"]
    url: str
    format: str = ""
    meta: Dict[str, Any] = ""


DataType = Union[float, int, str, bool,
                 dict, ArtifactDataType, List["DataType"]]


@dataclass
class Event:
    id: str  # event id
    type: str  # event type
    ts: str  # timestamp string
    # nanoseconds, high performance clock, used in perf events, start from 0 (program start point)
    perf_ts: int
    meta: Dict[str, Any]  # must be msgpack serializable
    data: Dict[str, DataType]

    def write_json(self, f):
        d = dataclasses.asdict(self)
        json.dump(d, f)

    def get_meta(self, key: str, default=None):
        return self.meta.get(key, default)
