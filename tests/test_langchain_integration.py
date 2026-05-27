"""
KAGE x LangChain callback handler tests.

Stubs `langchain_core.callbacks.BaseCallbackHandler` so the integration runs
without LangChain installed. Verifies that LangChain run_ids map to KAGE
span_ids and that parent_run_id is translated to parent_span_id.
"""
import importlib
import json
import shutil
import sys
import tempfile
import time
import types
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest


@pytest.fixture(scope="module")
def lc_stub():
    """Install a fake langchain_core.callbacks module before importing KAGE
    bridge. Yields the reloaded integration module."""
    if "langchain_core" not in sys.modules:
        sys.modules["langchain_core"] = types.ModuleType("langchain_core")
        callbacks_mod = types.ModuleType("langchain_core.callbacks")

        class BaseCallbackHandler:
            pass

        callbacks_mod.BaseCallbackHandler = BaseCallbackHandler
        sys.modules["langchain_core.callbacks"] = callbacks_mod

    import kage.integrations.langchain as lc_mod
    importlib.reload(lc_mod)
    yield lc_mod
    sys.modules.pop("langchain_core.callbacks", None)
    sys.modules.pop("langchain_core", None)
    importlib.reload(lc_mod)


@pytest.fixture
def tmp_logger(lc_stub):
    from kage import KageLogger, set_default_logger
    base = tempfile.mkdtemp(prefix="kage-lc-")
    lg = KageLogger(base_path=base, pipeline_name="lc_pipeline",
                    platform="langchain")
    set_default_logger(lg)
    yield lg, Path(base)
    shutil.rmtree(base, ignore_errors=True)


def _read_events(base, event_type):
    out = []
    for f in base.glob(f"**/event_type={event_type}/**/part-*.jsonl"):
        for line in f.read_text().splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def _wait_for(base, event_type, timeout=2.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if list(base.glob(f"**/event_type={event_type}/**/part-*.jsonl")):
            return True
        time.sleep(0.05)
    return False


# --------------------------------------------------------------------------- #


def test_chain_start_end_emits_running_and_success(tmp_logger, lc_stub):
    lg, base = tmp_logger
    handler = lc_stub.KageLangChainCallback(logger=lg, agent_name="demo")

    run_id = uuid4()
    handler.on_chain_start({"name": "QA_Chain"}, {"query": "hi"}, run_id=run_id)
    handler.on_chain_end({"answer": "hi back"}, run_id=run_id)

    assert _wait_for(base, "task_run")
    events = _read_events(base, "task_run")
    running = [e for e in events if e["status"] == "RUNNING"]
    success = [e for e in events if e["status"] == "SUCCESS"]
    assert running and running[0]["kind"] == "chain"
    assert success and success[0]["task_name"] == "QA_Chain"


def test_parent_run_id_maps_to_parent_span_id(tmp_logger, lc_stub):
    lg, base = tmp_logger
    handler = lc_stub.KageLangChainCallback(logger=lg)

    parent = uuid4()
    child = uuid4()
    handler.on_chain_start({"name": "outer"}, {}, run_id=parent)
    handler.on_tool_start({"name": "search"}, "python", run_id=child,
                          parent_run_id=parent)
    handler.on_tool_end("results", run_id=child)
    handler.on_chain_end({}, run_id=parent)

    spans = _read_events(base, "task_run")
    running_by_name = {s["task_name"]: s for s in spans if s["status"] == "RUNNING"}
    assert running_by_name["search"]["parent_span_id"] == running_by_name["outer"]["task_run_id"]
    assert running_by_name["outer"]["parent_span_id"] is None


def test_llm_end_captures_token_usage(tmp_logger, lc_stub):
    lg, base = tmp_logger
    handler = lc_stub.KageLangChainCallback(logger=lg)

    run_id = uuid4()
    handler.on_llm_start({"name": "openai"}, prompts=["hello"], run_id=run_id,
                         invocation_params={"model": "gpt-4o-mini"})

    fake_response = SimpleNamespace(
        llm_output={"token_usage": {"prompt_tokens": 12,
                                    "completion_tokens": 18,
                                    "total_tokens": 30}},
        generations=[],
    )
    handler.on_llm_end(fake_response, run_id=run_id)

    spans = _read_events(base, "task_run")
    success = [s for s in spans
               if s["kind"] == "llm_call" and s["status"] == "SUCCESS"]
    assert success
    cf = success[0]["custom_fields"]
    assert cf["prompt_tokens"] == 12
    assert cf["completion_tokens"] == 18
    assert cf["total_tokens"] == 30
    assert cf["model"] == "gpt-4o-mini"


def test_tool_error_emits_failed(tmp_logger, lc_stub):
    lg, base = tmp_logger
    handler = lc_stub.KageLangChainCallback(logger=lg)

    run_id = uuid4()
    handler.on_tool_start({"name": "calculator"}, "1/0", run_id=run_id)
    try:
        raise ZeroDivisionError("division by zero")
    except ZeroDivisionError as exc:
        handler.on_tool_error(exc, run_id=run_id)

    spans = _read_events(base, "task_run")
    failed = [s for s in spans if s["status"] == "FAILED"]
    assert failed
    cf = failed[0]["custom_fields"]
    assert cf["error_type"] == "ZeroDivisionError"
    assert "division by zero" in cf["error_message"]
    assert cf["stack_trace"]


def test_retriever_records_doc_count(tmp_logger, lc_stub):
    lg, base = tmp_logger
    handler = lc_stub.KageLangChainCallback(logger=lg)

    run_id = uuid4()
    handler.on_retriever_start({"name": "vectorstore"}, "search query",
                               run_id=run_id)
    handler.on_retriever_end(["doc1", "doc2", "doc3", "doc4"], run_id=run_id)

    spans = _read_events(base, "task_run")
    success = [s for s in spans
               if s["kind"] == "retrieval" and s["status"] == "SUCCESS"]
    assert success and success[0]["custom_fields"]["doc_count"] == 4


def test_unknown_end_event_is_noop(tmp_logger, lc_stub):
    lg, base = tmp_logger
    handler = lc_stub.KageLangChainCallback(logger=lg)
    # End without matching start - should not crash
    handler.on_chain_end({"x": 1}, run_id=uuid4())
    # No task_run files necessarily, but it shouldn't raise


def test_handler_raises_without_langchain():
    """Force re-import without the stub installed to verify the helpful error."""
    sys.modules.pop("langchain_core.callbacks", None)
    sys.modules.pop("langchain_core", None)
    import kage.integrations.langchain as lc_mod
    importlib.reload(lc_mod)
    try:
        with pytest.raises(ImportError, match="langchain_core is not installed"):
            lc_mod.KageLangChainCallback()
    finally:
        # Reinstall the stub so other tests don't break
        sys.modules.setdefault("langchain_core", types.ModuleType("langchain_core"))
        cm = types.ModuleType("langchain_core.callbacks")

        class BaseCallbackHandler:
            pass

        cm.BaseCallbackHandler = BaseCallbackHandler
        sys.modules["langchain_core.callbacks"] = cm
        importlib.reload(lc_mod)
