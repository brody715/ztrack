from dataclasses import dataclass
import datetime
import logging
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional
from ztrack.datatype import ArtifactDataType, Event

lib_logger = logging.getLogger(__name__)


class EventReporter(object):
    def __init__(self, name: str, out_file_path: Path, num_buffers: int = 10) -> None:
        """

        :param: num_buffer - every {num_buffer} events, write
        """
        self._name = name
        self._out_file = out_file_path.open('w')
        self._num_buffers = num_buffers
        self._buffers: List[Event] = []

        self._evt_id = 0

    def record_event(self, evt: Event):
        evt.id = self._get_evt_id()
        self._buffers.append(evt)

        if len(self._buffers) > self._num_buffers:
            self._flush()

    def _flush(self):
        for evt in self._buffers:
            evt.write_json(self._out_file)
            self._out_file.write('\n')
        self._buffers = []

    def _get_evt_id(self):
        self._evt_id += 1
        return str(self._evt_id)

    def finalize(self):
        self._flush()
        self._out_file.close()


# (event, reporter) -> ()
EventRecordCallback = Callable[[Event, str], None]


class EventRecorder(object):
    def __init__(self, result_dir: Path) -> None:
        self._result_dir = result_dir
        self._artifacts_dir = self._result_dir / 'artifacts'
        self._reporters: Dict[str, EventReporter] = {}

        self._share_cnt = 0
        self._artifact_id = 1000
        self._result_dir.mkdir(parents=True, exist_ok=True)
        self._artifacts_dir.mkdir(exist_ok=True)

        self._event_record_callbacks: List[EventRecordCallback] = []

    def _get_artifact_id(self):
        self._artifact_id += 1
        return str(self._artifact_id)

    def save_artifact(self, data: Any, save_func: Callable[[Any, Path], None], prefix, format: str, meta: Dict) -> ArtifactDataType:
        if len(format) == 0:
            format = "bin"

        if len(prefix) == 0:
            prefix = "data"

        artifact_name = f'{prefix}_{self._get_artifact_id()}.{format}'
        artifact_path = self._artifacts_dir / artifact_name

        # save data
        save_func(data, artifact_path)

        return ArtifactDataType(
            _type_='artifact',
            url=artifact_name,
            format=format,
            meta=meta,
        )

    def register_callback(self, cb: EventRecordCallback):
        self._event_record_callbacks.append(cb)

    def record_event(self, evt: Event, reporter: str):
        self._get_reporter(reporter).record_event(evt)

        for callback in self._event_record_callbacks:
            try:
                callback(evt, reporter)
            except Exception:
                lib_logger.warn("failed to run callback", exc_info=True)

    def share(self) -> 'EventRecorder':
        self._share_cnt += 1
        return self

    def unshare(self):
        self._share_cnt -= 1
        if self._share_cnt == 0:
            self._finalize()

    def _finalize(self):
        for _, reporter in self._reporters.items():
            reporter.finalize()
        self._reporters = {}

    def __del__(self):
        self._finalize()

    def _get_reporter(self, name: str):
        reporter = self._reporters.get(name, None)
        if not reporter:
            reporter = EventReporter(
                name, self._result_dir / f'{name}.event.json')
            self._reporters[name] = reporter
        return reporter


@dataclass
class TrackerConfig:
    reporter: str
    meta: Dict

    def merge_config(self, new_cfg: 'TrackerConfig') -> 'TrackerConfig':
        reporter = new_cfg.reporter if len(
            new_cfg.reporter) != 0 else self.reporter
        return TrackerConfig(reporter, {**self.meta, **new_cfg.meta})


@dataclass
class TrackItem:
    fields: Optional[Dict]
    meta: Optional[Dict]


class Tracker(object):
    def __init__(self, recorder: EventRecorder, logger: logging.Logger, config: TrackerConfig, fields: Dict, perf_timer_ns: int) -> None:
        self._recorder = recorder
        self._config = config
        self._fields = fields
        self._perf_timer_ns = perf_timer_ns
        self._logger = logger

        # tracker local variables
        self._tracks_buf: List[TrackItem] = []  # for defer commit

    def clone(self):
        return Tracker(
            recorder=self._recorder.share(),
            config=self._config,
            fields=self._fields,
            logger=self._logger,
            perf_timer_ns=self._perf_timer_ns,
        )

    def with_config(self, reporter: str = "", meta: Optional[Dict] = None):
        if meta is None:
            meta = {}

        new_config = self._config.merge_config(TrackerConfig(reporter, meta))
        tracker = self.clone()
        tracker._config = new_config
        return tracker

    def with_fields(self, fields: Dict):
        new_fields = {**self._fields, **fields}
        tracker = self.clone()
        tracker._fields = new_fields
        return tracker

    def track(self, fields: Optional[Dict] = None, meta: Optional[Dict] = None, commit: bool = True):
        self._tracks_buf.append(TrackItem(fields, meta))

        if commit:
            fields = {}
            meta = {}
            for track in self._tracks_buf:
                if track.fields:
                    fields.update(track.fields)
                if track.meta:
                    meta.update(track.meta)
            self._tracks_buf = []

            evt = self._create_event(meta)
            evt.type = 'z.tk'
            evt.data = fields
            self._record_event(evt)

    def info(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.INFO, msg, exclude_fields, include_fields)

    def error(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.ERROR, msg, exclude_fields, include_fields)

    def warn(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.WARN, msg, exclude_fields, include_fields)

    def debug(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.DEBUG, msg, exclude_fields, include_fields)

    def log(self, level: int, msg: str, exclude_fields: None, include_fields: None):
        evt = self._create_event()
        evt.type = "z.log"
        evt.data['log.level'] = logging.getLevelName(level)
        evt.data['log.msg'] = msg
        self._record_event(evt)

        if self._logger.isEnabledFor(level):
            self._logger.log(level, "%s %s", msg, self._format_log_fields(
                exclude_fields, include_fields))

        return self

    def finalize(self):
        self._recorder.unshare()

    def Artifact(self, data: Any, save_func: Callable[[Any, Path], None], prefix: str = "", format: str = "bin", meta: Optional[Dict] = None) -> ArtifactDataType:
        """
        :param: data - data to save
        :param: save_func - (data, path) -> None, save_func to implement actual save logic
        :param: format - format of the artifact (@see ArtifactDataType)
        """
        if meta is None:
            meta = {}

        return self._recorder.save_artifact(data=data, save_func=save_func, prefix=prefix, format=format, meta=meta)

    def register_event_callback(self, cb: EventRecordCallback):
        self._recorder.register_callback(cb)

    def _format_log_fields(self, exclude_fields: None, include_fields: None) -> str:
        field_keys = set(self._fields.keys())
        if exclude_fields:
            field_keys -= set(exclude_fields)
        if include_fields:
            field_keys &= set(include_fields)

        return " ".join([f"{k}={self._fields[k]}" for k in field_keys])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.finalize()

    def __del__(self):
        self.finalize()

    def _record_event(self, evt: Event):
        reporter = self._config.reporter
        if len(reporter) == 0:
            reporter = "default"
        return self._recorder.record_event(evt, reporter)

    def _create_event(self, meta: Optional[Dict] = None) -> Event:
        if meta:
            meta.update(self._config.meta)
        else:
            meta = self._config.meta

        return Event(
            id="",
            type="",
            ts=datetime.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S %z"),
            perf_ts=time.perf_counter_ns() - self._perf_timer_ns,
            meta=meta,
            data={
                **self._fields,
            }
        )
