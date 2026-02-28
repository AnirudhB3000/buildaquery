from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Callable, Mapping

# ==================================================
# Observability Types
# ==================================================

QueryObserveHook = Callable[["QueryObservation"], None]
EventObserveHook = Callable[["ExecutionEvent"], None]
LabelMap = Mapping[str, str]


@dataclass(frozen=True)
class ObservabilitySettings:
    """
    Cross-dialect execution observability settings.
    """

    query_observer: QueryObserveHook | None = None
    event_observer: EventObserveHook | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryObservation:
    """
    Structured query execution observation payload.
    """

    dialect: str
    operation: str
    sql: str
    param_count: int
    duration_ms: float
    succeeded: bool
    in_transaction: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class ExecutionEvent:
    """
    Structured executor lifecycle event payload.
    """

    timestamp: str
    event: str
    dialect: str
    executor: str
    success: bool
    metadata: Mapping[str, Any] = field(default_factory=dict)
    operation: str | None = None
    query_id: str | None = None
    transaction_id: str | None = None
    savepoint_name: str | None = None
    connection_id: str | None = None
    duration_ms: float | None = None
    retry_attempt: int | None = None
    max_attempts: int | None = None
    backoff_ms: float | None = None
    error_type: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    retryable: bool | None = None


def execution_event_to_dict(event: ExecutionEvent) -> dict[str, Any]:
    """
    Converts an ExecutionEvent dataclass into a JSON-safe dictionary.
    """

    return {
        "timestamp": event.timestamp,
        "event": event.event,
        "dialect": event.dialect,
        "executor": event.executor,
        "success": event.success,
        "metadata": dict(event.metadata),
        "operation": event.operation,
        "query_id": event.query_id,
        "transaction_id": event.transaction_id,
        "savepoint_name": event.savepoint_name,
        "connection_id": event.connection_id,
        "duration_ms": event.duration_ms,
        "retry_attempt": event.retry_attempt,
        "max_attempts": event.max_attempts,
        "backoff_ms": event.backoff_ms,
        "error_type": event.error_type,
        "error_code": event.error_code,
        "error_message": event.error_message,
        "retryable": event.retryable,
    }


def make_json_event_logger(
    *,
    logger: logging.Logger,
    level: int = logging.INFO,
) -> EventObserveHook:
    """
    Builds an EventObserveHook that emits one JSON log line per ExecutionEvent.
    """

    def _log_event(event: ExecutionEvent) -> None:
        payload = execution_event_to_dict(event)
        logger.log(level, json.dumps(payload, separators=(",", ":"), sort_keys=True))

    return _log_event


def compose_event_observers(*observers: EventObserveHook) -> EventObserveHook:
    """
    Composes multiple event observers into a single observer.
    """

    def _composed(event: ExecutionEvent) -> None:
        for observer in observers:
            observer(event)

    return _composed


def _normalize_label(value: str | None, *, fallback: str) -> str:
    if value is None:
        return fallback
    stripped = value.strip()
    return stripped if stripped else fallback


def _event_labels(event: ExecutionEvent) -> dict[str, str]:
    return {
        "dialect": _normalize_label(event.dialect, fallback="unknown"),
        "executor": _normalize_label(event.executor, fallback="unknown"),
        "operation": _normalize_label(event.operation, fallback="unknown"),
        "event": _normalize_label(event.event, fallback="unknown"),
        "error_type": _normalize_label(event.error_type, fallback="none"),
    }


def _labels_key(labels: LabelMap) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(key), str(value)) for key, value in labels.items()))


@dataclass(frozen=True)
class MetricPoint:
    """
    Single metric point lookup result.
    """

    name: str
    labels: Mapping[str, str]
    value: int | float


class InMemoryMetricsAdapter:
    """
    In-memory metrics adapter for ExecutionEvent streams.
    """

    def __init__(self) -> None:
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
        self._histograms: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}

    def __call__(self, event: ExecutionEvent) -> None:
        labels = _event_labels(event)
        if event.event == "query.end":
            self._inc("buildaquery_queries_total", labels, 1)
            if not event.success:
                self._inc("buildaquery_query_failures_total", labels, 1)
            if event.duration_ms is not None:
                self._observe("buildaquery_query_duration_ms", labels, event.duration_ms)
            return

        if event.event == "retry.scheduled":
            self._inc("buildaquery_retries_total", labels, 1)
            return

        if event.event == "retry.giveup":
            self._inc("buildaquery_retry_giveups_total", labels, 1)
            return

        if event.event in {"txn.commit", "txn.rollback"} and event.duration_ms is not None:
            self._observe("buildaquery_txn_duration_ms", labels, event.duration_ms)
            return

        if event.event == "connection.acquire.end" and event.duration_ms is not None:
            self._observe("buildaquery_connection_acquire_ms", labels, event.duration_ms)

    def _inc(self, metric: str, labels: LabelMap, delta: int) -> None:
        key = (metric, _labels_key(labels))
        self._counters[key] = self._counters.get(key, 0) + delta

    def _observe(self, metric: str, labels: LabelMap, value: float) -> None:
        key = (metric, _labels_key(labels))
        bucket = self._histograms.setdefault(key, [])
        bucket.append(value)

    def counter_value(self, metric: str, labels: LabelMap) -> int:
        return self._counters.get((metric, _labels_key(labels)), 0)

    def histogram_values(self, metric: str, labels: LabelMap) -> list[float]:
        values = self._histograms.get((metric, _labels_key(labels)), [])
        return list(values)

    def counters(self) -> list[MetricPoint]:
        points: list[MetricPoint] = []
        for (name, label_key), value in self._counters.items():
            points.append(MetricPoint(name=name, labels=dict(label_key), value=value))
        return points

    def histograms(self) -> list[MetricPoint]:
        points: list[MetricPoint] = []
        for (name, label_key), values in self._histograms.items():
            for value in values:
                points.append(MetricPoint(name=name, labels=dict(label_key), value=value))
        return points


@dataclass(frozen=True)
class TraceEvent:
    """
    Structured span event captured by the in-memory tracing adapter.
    """

    timestamp: str
    name: str
    attributes: Mapping[str, Any] = field(default_factory=dict)


@dataclass
class TraceSpan:
    """
    Minimal in-memory trace span.
    """

    name: str
    start_timestamp: str
    attributes: dict[str, Any] = field(default_factory=dict)
    end_timestamp: str | None = None
    events: list[TraceEvent] = field(default_factory=list)
    status: str = "ok"

    def add_event(self, name: str, *, timestamp: str, attributes: Mapping[str, Any] | None = None) -> None:
        self.events.append(
            TraceEvent(
                timestamp=timestamp,
                name=name,
                attributes=dict(attributes or {}),
            )
        )


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class InMemoryTracingAdapter:
    """
    In-memory tracing adapter that derives spans from ExecutionEvent streams.
    """

    def __init__(self) -> None:
        self._active_query_spans: dict[str, TraceSpan] = {}
        self._active_transaction_spans: dict[str, TraceSpan] = {}
        self.completed_spans: list[TraceSpan] = []
        self.unscoped_events: list[TraceEvent] = []

    def __call__(self, event: ExecutionEvent) -> None:
        labels = _event_labels(event)
        timestamp = event.timestamp or _now_iso_utc()

        if event.event == "query.start" and event.query_id is not None:
            span = TraceSpan(
                name="db.query",
                start_timestamp=timestamp,
                attributes={
                    "db.system": labels["dialect"],
                    "db.operation": labels["operation"],
                    "buildaquery.executor": labels["executor"],
                    "buildaquery.query_id": event.query_id,
                },
            )
            self._active_query_spans[event.query_id] = span
            return

        if event.event == "query.end" and event.query_id is not None:
            span = self._active_query_spans.pop(event.query_id, None)
            if span is None:
                span = TraceSpan(
                    name="db.query",
                    start_timestamp=timestamp,
                    attributes={
                        "db.system": labels["dialect"],
                        "db.operation": labels["operation"],
                        "buildaquery.executor": labels["executor"],
                        "buildaquery.query_id": event.query_id,
                    },
                )
            span.end_timestamp = timestamp
            span.status = "ok" if event.success else "error"
            if event.duration_ms is not None:
                span.attributes["buildaquery.duration_ms"] = event.duration_ms
            if event.error_type is not None:
                span.attributes["error.type"] = event.error_type
            self.completed_spans.append(span)
            return

        if event.event == "txn.begin" and event.transaction_id is not None:
            self._active_transaction_spans[event.transaction_id] = TraceSpan(
                name="db.transaction",
                start_timestamp=timestamp,
                attributes={
                    "db.system": labels["dialect"],
                    "buildaquery.executor": labels["executor"],
                    "buildaquery.transaction_id": event.transaction_id,
                },
            )
            return

        if event.event in {"txn.commit", "txn.rollback"} and event.transaction_id is not None:
            span = self._active_transaction_spans.pop(event.transaction_id, None)
            if span is None:
                span = TraceSpan(
                    name="db.transaction",
                    start_timestamp=timestamp,
                    attributes={
                        "db.system": labels["dialect"],
                        "buildaquery.executor": labels["executor"],
                        "buildaquery.transaction_id": event.transaction_id,
                    },
                )
            span.end_timestamp = timestamp
            span.status = "ok" if event.event == "txn.commit" else "error"
            span.attributes["buildaquery.outcome"] = event.event
            if event.duration_ms is not None:
                span.attributes["buildaquery.duration_ms"] = event.duration_ms
            self.completed_spans.append(span)
            return

        self._route_event_to_span_or_unscoped(event=event, timestamp=timestamp)

    def _route_event_to_span_or_unscoped(self, *, event: ExecutionEvent, timestamp: str) -> None:
        attrs = execution_event_to_dict(event)
        if event.query_id is not None and event.query_id in self._active_query_spans:
            self._active_query_spans[event.query_id].add_event(
                event.event,
                timestamp=timestamp,
                attributes=attrs,
            )
            return
        if event.transaction_id is not None and event.transaction_id in self._active_transaction_spans:
            self._active_transaction_spans[event.transaction_id].add_event(
                event.event,
                timestamp=timestamp,
                attributes=attrs,
            )
            return
        self.unscoped_events.append(
            TraceEvent(
                timestamp=timestamp,
                name=event.event,
                attributes=attrs,
            )
        )
