import logging
from pathlib import Path
from typing import Union
from ztrack.tracker import Tracker, TrackerConfig
from ztrack.datatype import Event


def str_datetime() -> str:
    import time
    return time.strftime("%Y%m%d-%H%M%S", time.localtime())


def create(result_dir: Union[str, Path]) -> Tracker:
    from ztrack.tracker import EventRecorder
    import time

    result_dir = Path(result_dir)
    recorder = EventRecorder(result_dir)

    return Tracker(
        recorder=recorder,
        logger=logging.getLogger('ztracker'),
        config=TrackerConfig("default", {}),
        fields={},
        perf_timer_ns=time.perf_counter_ns(),
    )


__all__ = [
    "Tracker",
    "Event",
    "create",
    "str_datetime"
]
