import logging
from pathlib import Path
from typing import Any, Union
from ztrack.reader import Reader
from ztrack.tracker import Tracker, TrackerSetting
from ztrack.datatype import Event


def create(result_dir: Union[str, Path], dry_run=False) -> Tracker:
    from ztrack.tracker import EventRecorder
    import time

    result_dir = Path(result_dir)
    recorder = EventRecorder(result_dir, dry_run=dry_run)

    return Tracker(
        recorder=recorder,
        logger=logging.getLogger('ztracker'),
        settings=TrackerSetting("default", {}),
        fields={},
        perf_timer_ns=time.perf_counter_ns(),
    )


def reader(result_dir: Union[str, Path]) -> Reader:
    result_dir = Path(result_dir)

    return Reader(
        result_dir=result_dir,
    )


__all__ = [
    "Tracker",
    "Reader",
    "Event",
    "create",
    "reader",
    "str_datetime",
    "save_yaml"
]


# Utility functions

def str_datetime() -> str:
    import time
    return time.strftime("%Y%m%d-%H%M%S", time.localtime())


def save_yaml(data: Any, path: Path):
    import yaml
    with path.open("w") as f:
        yaml.dump(data, f)
