"""
KAGE x LangChain / LangGraph
============================

A `BaseCallbackHandler` subclass that turns LangChain / LangGraph runtime
events into KAGE span events. Drop into any chain / agent / runnable:

    from langchain_openai import ChatOpenAI
    from kage import configure
    from kage.integrations.langchain import KageLangChainCallback

    configure(base_path="./kage-logs", pipeline_name="my_agent")

    llm = ChatOpenAI(model="gpt-4o-mini")
    handler = KageLangChainCallback()

    result = llm.invoke("hello", config={"callbacks": [handler]})

Each LangChain run_id becomes one KAGE task_run; parent_run_id maps to KAGE
parent_span_id, so the full call tree shows up in the event stream.

LangChain is an optional dependency. This module imports cleanly without it;
only instantiating `KageLangChainCallback` outside LangChain raises an
ImportError with a helpful message.
"""
from __future__ import annotations

import time
import traceback
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from ..core import KageLogger
from ..decorators import get_default_logger

try:
    from langchain_core.callbacks import BaseCallbackHandler  # type: ignore
    _HAS_LANGCHAIN = True
except ImportError:
    _HAS_LANGCHAIN = False

    class BaseCallbackHandler:  # type: ignore[no-redef]
        """Stub so the class definition imports cleanly."""
        pass


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


class KageLangChainCallback(BaseCallbackHandler):
    """Bridges LangChain callbacks -> KAGE task_run events.

    Args:
        logger: KageLogger to write to. Defaults to KAGE's default logger.
        agent_name: Optional name used as a default `task_name` when
            LangChain provides only a run_id.
    """

    def __init__(
        self,
        logger: Optional[KageLogger] = None,
        agent_name: Optional[str] = None,
    ):
        if not _HAS_LANGCHAIN:
            raise ImportError(
                "langchain_core is not installed. Install with "
                "`pip install langchain-core` (or langchain) to use "
                "KageLangChainCallback."
            )
        self.logger = logger or get_default_logger()
        self.agent_name = agent_name or "langchain_run"
        # run_id (str) -> {span_id, name, kind, parent, t0}
        self._spans: Dict[str, Dict[str, Any]] = {}

    # ----- helpers ----------------------------------------------------------

    def _key(self, run_id: UUID) -> str:
        return str(run_id)

    def _open(
        self,
        run_id: UUID,
        parent_run_id: Optional[UUID],
        name: str,
        kind: str,
        **fields: Any,
    ) -> None:
        key = self._key(run_id)
        parent_span_id = (
            self._spans.get(self._key(parent_run_id), {}).get("span_id")
            if parent_run_id is not None
            else None
        )
        span_id = str(uuid.uuid4())
        self._spans[key] = {
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "name": name,
            "kind": kind,
            "t0": time.perf_counter(),
            "fields": dict(fields),
        }
        self.logger._safe_emit({
            "event_type": "task_run",
            "task_run_id": span_id,
            "job_run_id": self.logger.active_runs.get("job") or self.agent_name,
            "parent_span_id": parent_span_id,
            "kind": kind,
            "task_name": name,
            "status": "RUNNING",
            "start_time": _now_iso(),
            "custom_fields": dict(fields),
        })

    def _close(self, run_id: UUID, status: str, **fields: Any) -> None:
        key = self._key(run_id)
        span = self._spans.pop(key, None)
        if span is None:
            return
        latency_ms = int((time.perf_counter() - span["t0"]) * 1000)
        merged = {**span["fields"], **fields}
        self.logger._safe_emit({
            "event_type": "task_run",
            "task_run_id": span["span_id"],
            "job_run_id": self.logger.active_runs.get("job") or self.agent_name,
            "parent_span_id": span["parent_span_id"],
            "kind": span["kind"],
            "task_name": span["name"],
            "status": status,
            "end_time": _now_iso(),
            "latency_ms": latency_ms,
            "custom_fields": merged,
        })

    @staticmethod
    def _error_fields(error: BaseException) -> Dict[str, Any]:
        return {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": traceback.format_exc(),
        }

    # ----- chain ------------------------------------------------------------

    def on_chain_start(
        self,
        serialized: Optional[Dict[str, Any]],
        inputs: Dict[str, Any],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        name = (serialized or {}).get("name") or kwargs.get("name") or "chain"
        self._open(run_id, parent_run_id, name=name, kind="chain")

    def on_chain_end(self, outputs: Dict[str, Any], *, run_id: UUID, **kwargs: Any) -> None:
        self._close(run_id, "SUCCESS")

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._close(run_id, "FAILED", **self._error_fields(error))

    # ----- LLM --------------------------------------------------------------

    def on_llm_start(
        self,
        serialized: Optional[Dict[str, Any]],
        prompts: List[str],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        invocation_params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        model = (
            (invocation_params or {}).get("model")
            or (invocation_params or {}).get("model_name")
            or (serialized or {}).get("name")
            or "llm"
        )
        self._open(
            run_id, parent_run_id,
            name=model, kind="llm_call",
            model=model,
            prompt_count=len(prompts) if prompts else 0,
        )

    def on_chat_model_start(
        self,
        serialized: Optional[Dict[str, Any]],
        messages: List[List[Any]],
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        invocation_params: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        model = (
            (invocation_params or {}).get("model")
            or (invocation_params or {}).get("model_name")
            or (serialized or {}).get("name")
            or "chat_model"
        )
        msg_count = sum(len(m) for m in messages) if messages else 0
        self._open(
            run_id, parent_run_id,
            name=model, kind="llm_call",
            model=model,
            message_count=msg_count,
        )

    def on_llm_end(self, response: Any, *, run_id: UUID, **kwargs: Any) -> None:
        usage: Dict[str, Any] = {}
        llm_output = getattr(response, "llm_output", None) or {}
        token_usage = llm_output.get("token_usage") if isinstance(llm_output, dict) else None
        if token_usage:
            for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                if key in token_usage:
                    usage[key] = token_usage[key]
        if not usage:
            # newer langchain: response.usage_metadata on the generations
            try:
                gens = getattr(response, "generations", []) or []
                for gen_list in gens:
                    for gen in gen_list:
                        meta = getattr(gen, "message", None)
                        if meta is not None:
                            um = getattr(meta, "usage_metadata", None)
                            if um:
                                usage["prompt_tokens"] = um.get("input_tokens")
                                usage["completion_tokens"] = um.get("output_tokens")
                                usage["total_tokens"] = um.get("total_tokens")
                                break
            except Exception:
                pass
        self._close(run_id, "SUCCESS", **{k: v for k, v in usage.items() if v is not None})

    def on_llm_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._close(run_id, "FAILED", **self._error_fields(error))

    # ----- tool -------------------------------------------------------------

    def on_tool_start(
        self,
        serialized: Optional[Dict[str, Any]],
        input_str: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        name = (serialized or {}).get("name") or kwargs.get("name") or "tool"
        self._open(
            run_id, parent_run_id, name=name, kind="tool",
            tool_input=str(input_str)[:1000],
        )

    def on_tool_end(self, output: Any, *, run_id: UUID, **kwargs: Any) -> None:
        self._close(run_id, "SUCCESS", tool_output=str(output)[:1000])

    def on_tool_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._close(run_id, "FAILED", **self._error_fields(error))

    # ----- retriever (LangChain >= 0.1) -------------------------------------

    def on_retriever_start(
        self,
        serialized: Optional[Dict[str, Any]],
        query: str,
        *,
        run_id: UUID,
        parent_run_id: Optional[UUID] = None,
        **kwargs: Any,
    ) -> None:
        name = (serialized or {}).get("name") or "retriever"
        self._open(run_id, parent_run_id, name=name, kind="retrieval",
                   query=str(query)[:500])

    def on_retriever_end(self, documents: Any, *, run_id: UUID, **kwargs: Any) -> None:
        try:
            doc_count = len(documents)
        except TypeError:
            doc_count = 0
        self._close(run_id, "SUCCESS", doc_count=doc_count)

    def on_retriever_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        self._close(run_id, "FAILED", **self._error_fields(error))
