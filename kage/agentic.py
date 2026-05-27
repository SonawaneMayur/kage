"""
KAGE Agentic Observability
==========================

Span-based observability for agentic, sequential, and parallel workflows that
don't fit the medallion model. Same JSONL event stream — different `kind`.

Decorators (sync and async both supported automatically):
    @agent("research_agent")    - top-level agent run -> job_run + root span
    @step("plan")               - generic step
    @tool("web_search")         - tool invocation (kind="tool")
    @llm_call(model="...")      - LLM call (kind="llm_call"), pair with log_llm_usage

Context manager:
    with kage_span("retrieve", kind="retrieval", k=10) as span:
        chunks = retrieve(query, k=10)
        span.add(chunks_returned=len(chunks))

Helpers:
    current_span()              - the active span (or None)
    log_llm_usage(...)          - attach token / cost / model to the active span
    log_metric(**fields)        - attach arbitrary fields to the active span

Concurrency model:
    Parent/child linkage uses contextvars, so:
      - asyncio.gather(...)              -> children share the parent automatically
      - nested sync calls                 -> children see the enclosing parent
      - ThreadPoolExecutor                -> wrap with contextvars.copy_context().run

Every event is a normal KAGE event (event_type=job_run or task_run), so
existing analytics queries and storage continue to work unchanged.
"""
from __future__ import annotations

import contextvars
import functools
import inspect
import time
import traceback
import uuid
from datetime import datetime
from typing import Any, Callable, Optional

from .core import KageLogger
from .decorators import get_default_logger


# Stack of currently-active span objects (a tuple stored in a contextvar so
# parent/child relationships propagate correctly across awaits and across
# child coroutines spawned via asyncio.gather).
_active_span_stack: contextvars.ContextVar = contextvars.ContextVar(
    "kage_active_span_stack", default=()
)

# Active job_run_id for the surrounding @agent (per-task in the contextvar
# sense, so parallel agents do not clobber each other).
_active_job_id: contextvars.ContextVar = contextvars.ContextVar(
    "kage_active_job_id", default=None
)


def current_span() -> Optional["_SpanContext"]:
    """Return the deepest active span, or None if none is open."""
    stack = _active_span_stack.get()
    return stack[-1] if stack else None


def _current_parent_id() -> Optional[str]:
    span = current_span()
    return span.span_id if span is not None else None


def _error_fields(exc: BaseException) -> dict:
    return {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "stack_trace": traceback.format_exc(),
    }


def _emit_span(logger: KageLogger, span: "_SpanContext", *,
               status: str, **extra) -> None:
    event = {
        "event_type": "task_run",
        "task_run_id": span.span_id,
        "job_run_id": _active_job_id.get() or logger.active_runs.get("job") or "no_job",
        "parent_span_id": span.parent_span_id,
        "kind": span.kind,
        "task_name": span.name,
        "status": status,
    }
    if status == "RUNNING":
        event["start_time"] = span.start_time
    else:
        event["end_time"] = datetime.utcnow().isoformat()
        event["latency_ms"] = int((time.perf_counter() - span.t0) * 1000)
    fields = {**span.fields, **extra}
    if fields:
        event["custom_fields"] = fields
    logger._safe_emit(event)


def _emit_job(logger: KageLogger, *, job_id: str, name: str,
              status: str, **fields) -> None:
    event = {
        "event_type": "job_run",
        "job_run_id": job_id,
        "job_name": name,
        "kind": "agent",
        "status": status,
    }
    if status == "RUNNING":
        event["start_time"] = datetime.utcnow().isoformat()
    else:
        event["end_time"] = datetime.utcnow().isoformat()
    if fields:
        event["custom_fields"] = fields
    logger._safe_emit(event)


class _SpanContext:
    """A single span. Use via `kage_span(...)` or the decorators.

    Supports both `with` and `async with`. On exception, logs FAILED + stack
    trace and re-raises (never swallows errors).
    """

    __slots__ = ("name", "kind", "logger", "fields",
                 "span_id", "parent_span_id", "start_time", "t0",
                 "_span_token")

    def __init__(self, name: str, kind: str, *,
                 logger: Optional[KageLogger] = None, fields: Optional[dict] = None):
        self.name = name
        self.kind = kind
        self.logger = logger
        self.fields = dict(fields or {})
        self.span_id = str(uuid.uuid4())
        self.parent_span_id: Optional[str] = None
        self.start_time: Optional[str] = None
        self.t0: float = 0.0
        self._span_token = None

    # ----- enter/exit (shared by sync + async) ------------------------------

    def _enter(self) -> "_SpanContext":
        self.logger = self.logger or get_default_logger()
        self.parent_span_id = _current_parent_id()
        self.start_time = datetime.utcnow().isoformat()
        self.t0 = time.perf_counter()
        new_stack = _active_span_stack.get() + (self,)
        self._span_token = _active_span_stack.set(new_stack)
        _emit_span(self.logger, self, status="RUNNING")
        return self

    def _exit(self, exc: Optional[BaseException]) -> bool:
        try:
            if exc is None:
                _emit_span(self.logger, self, status="SUCCESS")
            else:
                _emit_span(self.logger, self, status="FAILED", **_error_fields(exc))
        finally:
            if self._span_token is not None:
                _active_span_stack.reset(self._span_token)
                self._span_token = None
        return False  # never suppress

    # ----- sync context manager ---------------------------------------------

    def __enter__(self):
        return self._enter()

    def __exit__(self, exc_type, exc, tb):
        return self._exit(exc)

    # ----- async context manager --------------------------------------------

    async def __aenter__(self):
        return self._enter()

    async def __aexit__(self, exc_type, exc, tb):
        return self._exit(exc)

    # ----- mutation while open ---------------------------------------------

    def add(self, **fields: Any) -> None:
        """Attach extra fields to be included on the end event."""
        self.fields.update(fields)


def kage_span(name: str, *, kind: str = "step",
              logger: Optional[KageLogger] = None, **fields) -> _SpanContext:
    """Open a span as a context manager. See module docstring for usage."""
    return _SpanContext(name, kind, logger=logger, fields=fields)


def log_llm_usage(*, prompt_tokens: Optional[int] = None,
                  completion_tokens: Optional[int] = None,
                  total_tokens: Optional[int] = None,
                  cost_usd: Optional[float] = None,
                  model: Optional[str] = None,
                  **extra: Any) -> None:
    """Attach LLM-call metrics to the currently-active span.

    Computes `total_tokens` from prompt + completion if not given.
    No-op if there is no active span.
    """
    span = current_span()
    if span is None:
        return
    fields: dict = {}
    if prompt_tokens is not None:
        fields["prompt_tokens"] = prompt_tokens
    if completion_tokens is not None:
        fields["completion_tokens"] = completion_tokens
    if total_tokens is not None:
        fields["total_tokens"] = total_tokens
    elif prompt_tokens is not None and completion_tokens is not None:
        fields["total_tokens"] = prompt_tokens + completion_tokens
    if cost_usd is not None:
        fields["cost_usd"] = cost_usd
    if model is not None:
        fields["model"] = model
    fields.update(extra)
    span.add(**fields)


def log_metric(**fields: Any) -> None:
    """Attach arbitrary fields to the currently-active span."""
    span = current_span()
    if span is not None:
        span.add(**fields)


# ---------------------------------------------------------------------------
# Decorators


def _build_span_decorator(default_kind: str):
    """Make @step / @tool / @llm_call. Works for sync and async functions."""

    def factory(name=None, *, logger: Optional[KageLogger] = None, **default_fields):
        def decorator(func):
            span_name = name if isinstance(name, str) else func.__name__
            extra = dict(default_fields)

            if inspect.iscoroutinefunction(func):
                @functools.wraps(func)
                async def async_wrapper(*args, **kwargs):
                    async with _SpanContext(span_name, default_kind,
                                            logger=logger, fields=extra):
                        return await func(*args, **kwargs)
                return async_wrapper

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                with _SpanContext(span_name, default_kind,
                                  logger=logger, fields=extra):
                    return func(*args, **kwargs)
            return sync_wrapper

        if callable(name):  # @decorator (no parens)
            func, name = name, None
            return decorator(func)
        return decorator
    return factory


step = _build_span_decorator("step")
tool = _build_span_decorator("tool")
llm_call = _build_span_decorator("llm_call")


def agent(name=None, *, logger: Optional[KageLogger] = None, **default_fields):
    """Wrap an agent invocation.

    Emits a `job_run` start + end (with `kind=agent`) and runs the function
    body inside a root span so all nested @step / @tool / @llm_call calls
    correlate up to one agent run via contextvars.
    """

    def decorator(func):
        agent_name = name if isinstance(name, str) else func.__name__

        def _begin(lg: KageLogger):
            job_id = str(uuid.uuid4())
            job_token = _active_job_id.set(job_id)
            _emit_job(lg, job_id=job_id, name=agent_name, status="RUNNING",
                      **default_fields)
            return job_id, job_token

        def _end(lg: KageLogger, job_id: str, job_token, exc: Optional[BaseException]):
            if exc is None:
                _emit_job(lg, job_id=job_id, name=agent_name, status="SUCCESS",
                          **default_fields)
            else:
                _emit_job(lg, job_id=job_id, name=agent_name, status="FAILED",
                          **{**default_fields, **_error_fields(exc)})
            _active_job_id.reset(job_token)

        if inspect.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                lg = logger or get_default_logger()
                job_id, job_token = _begin(lg)
                try:
                    async with _SpanContext(agent_name, "agent",
                                            logger=lg, fields=default_fields):
                        result = await func(*args, **kwargs)
                    _end(lg, job_id, job_token, None)
                    return result
                except Exception as exc:
                    _end(lg, job_id, job_token, exc)
                    raise
            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            lg = logger or get_default_logger()
            job_id, job_token = _begin(lg)
            try:
                with _SpanContext(agent_name, "agent",
                                  logger=lg, fields=default_fields):
                    result = func(*args, **kwargs)
                _end(lg, job_id, job_token, None)
                return result
            except Exception as exc:
                _end(lg, job_id, job_token, exc)
                raise
        return sync_wrapper

    if callable(name):  # @agent (no parens)
        func, name = name, None
        return decorator(func)
    return decorator
