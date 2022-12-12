import contextlib
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
        self._out_file = out_file_path.open('a+')
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
        self._out_file.flush()

    def _get_evt_id(self):
        self._evt_id += 1
        return str(self._evt_id)

    def finalize(self):
        self._flush()
        self._out_file.close()


# (event, reporter) -> ()
EventRecordCallback = Callable[[Event, str], None]


class EventRecorder(object):
    def __init__(self, result_dir: Path, dry_run: bool) -> None:
        self._result_dir = result_dir
        self._artifacts_dir = self._result_dir / 'artifacts'
        self._reporters: Dict[str, EventReporter] = {}

        self._share_cnt = 0
        self._artifact_id = 1000
        self._span_id = 2000

        self._dry_run = dry_run

        if not self._dry_run:
            self._result_dir.mkdir(parents=True, exist_ok=True)
            self._artifacts_dir.mkdir(exist_ok=True)

        self._event_record_callbacks: List[EventRecordCallback] = []

    def _get_artifact_id(self):
        self._artifact_id += 1
        return str(self._artifact_id)

    def next_span_id(self):
        self._span_id += 1
        return str(self._span_id)

    def save_artifact(self, data: Any, save_func: Callable[[Any, Path], None], prefix, persist_name: str, format: str, meta: Dict) -> ArtifactDataType:
        if len(format) == 0:
            format = "bin"

        if len(prefix) == 0:
            prefix = "data"

        name = f'{prefix}_{self._get_artifact_id()}'
        if persist_name:
            name = persist_name

        artifact_name = f'{name}.{format}'
        artifact_path = self._artifacts_dir / artifact_name

        if not self._dry_run:
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
        if not self._dry_run:
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
class TrackerSetting:
    reporter: str
    meta: Dict
    record_log_event: Optional[bool] = None

    def merge(self, new_cfg: 'TrackerSetting') -> 'TrackerSetting':
        reporter = new_cfg.reporter if len(
            new_cfg.reporter) != 0 else self.reporter

        record_log_event = self.record_log_event
        if new_cfg.record_log_event is not None:
            record_log_event = new_cfg.record_log_event

        return TrackerSetting(reporter, {**self.meta, **new_cfg.meta}, record_log_event)


@dataclass
class TrackItem:
    fields: Optional[Dict]
    meta: Optional[Dict]


class Tracker(object):
    def __init__(self, recorder: EventRecorder, logger: logging.Logger, settings: TrackerSetting, fields: Dict, perf_timer_ns: int) -> None:
        self._recorder = recorder
        self._settings = settings
        self._fields = fields
        self._perf_timer_ns = perf_timer_ns
        self._logger = logger

        # tracker local variables
        self._tracks_buf: List[TrackItem] = []  # for defer commit

    def clone(self):
        return Tracker(
            recorder=self._recorder.share(),
            settings=self._settings,
            fields=self._fields,
            logger=self._logger,
            perf_timer_ns=self._perf_timer_ns,
        )

    def with_settings(self, reporter: str = "", meta: Optional[Dict] = None, record_log_event: Optional[bool] = None):
        if meta is None:
            meta = {}

        new_settings = self._settings.merge(
            TrackerSetting(reporter, meta, record_log_event))
        tracker = self.clone()
        tracker._settings = new_settings
        return tracker

    def with_fields(self, fields: Dict):
        new_fields = {**self._fields, **fields}
        tracker = self.clone()
        tracker._fields = new_fields
        return tracker

    def track(self, fields: Optional[Dict] = None, meta: Optional[Dict] = None, commit: bool = True):
        self._tracks_buf.append(TrackItem(fields, meta))

        if commit:
            fields = {**self._fields}
            meta = {**self._settings.meta}
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

        return self

    @contextlib.contextmanager
    def span(self, name: str, log_end_span: bool = False):
        span_id = self._recorder.next_span_id()
        tracker = self.with_settings(meta={
            'span.id': span_id,
            'span.name': name,
        })
        tracker._record_event(self._create_event(meta={
            'span.id': span_id,
            'span.name': name,
            'span.event': 'started'
        }, type='z.span'))

        start_time_ns = time.perf_counter_ns()
        try:

            yield tracker

        finally:
            elapsed_ns = time.perf_counter_ns() - start_time_ns
            elapsed_ms = (elapsed_ns / 10 ** 6) * 100 // 100
            if log_end_span:
                tracker.with_fields({
                    'span.id': span_id,
                    'span.name': name,
                    'span.elapsed_ms': elapsed_ms,
                }).info('span end')

            tracker._record_event(self._create_event(meta={
                'span.id': span_id,
                'span.name': name,
                'span.event': 'stopped',
                'span.elapsed_ms': elapsed_ms,
            }, type='z.span'))

    def info(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.INFO, msg, exclude_fields, include_fields)

    def error(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.ERROR, msg, exclude_fields, include_fields)

    def warn(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.WARN, msg, exclude_fields, include_fields)

    def debug(self, msg, exclude_fields: None = None, include_fields: None = None):
        return self.log(logging.DEBUG, msg, exclude_fields, include_fields)

    def log(self, level: int, msg: str, exclude_fields: None, include_fields: None):
        if self._settings.record_log_event:
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

    def Artifact(self, data: Any, save_func: Callable[[Any, Path], None], prefix: str = "", persist_name: str = "", format: str = "bin", meta: Optional[Dict] = None) -> ArtifactDataType:
        """
        :param: data - data to save
        :param: save_func - (data, path) -> None, save_func to implement actual save logic
        :param: format - format of the artifact (@see ArtifactDataType)
        """
        if meta is None:
            meta = {}

        return self._recorder.save_artifact(data=data, save_func=save_func, prefix=prefix, persist_name=persist_name, format=format, meta=meta)

    def track_config(self, data: Any, name: str):
        def yaml_saver(data: Any, path: Path):
            import yaml
            with open(path, 'w') as f:
                yaml.safe_dump(data, f)

        data = self.Artifact(data, save_func=yaml_saver,
                             persist_name=name, format='yaml')
        self.track({
            'config': data
        }, meta={'z.type': 'config'})

    def register_event_callback(self, cb: EventRecordCallback):
        self._recorder.register_callback(cb)

    def _format_log_fields(self, exclude_fields: None, include_fields: None) -> str:
        field_keys = set(self._fields.keys())
        if exclude_fields:
            field_keys -= set(exclude_fields)
        if include_fields:
            field_keys &= set(include_fields)

        fields = []

        for k in field_keys:
            value = self._fields[k]
            if isinstance(value, str):
                value = f"\"{value}\""

            fields.append(f"{k}={value}")

        return " ".join(fields)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.finalize()

    def __del__(self):
        self.finalize()

    def _record_event(self, evt: Event):
        reporter = self._settings.reporter
        if len(reporter) == 0:
            reporter = "default"
        return self._recorder.record_event(evt, reporter)

    def _create_event(self, meta: Optional[Dict] = None, type: str = "") -> Event:
        if meta:
            meta.update(self._settings.meta)
        else:
            meta = self._settings.meta

        return Event(
            id="",
            type=type,
            ts=datetime.datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S %z"),
            perf_ts=time.perf_counter_ns() - self._perf_timer_ns,
            meta=meta,
            data={
                **self._fields,
            }
        )
