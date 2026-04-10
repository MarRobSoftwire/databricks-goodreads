from types import SimpleNamespace

import pytest

from job_status import format_run_status


# ---------------------------------------------------------------------------
# Helpers — build lightweight fake SDK objects
# ---------------------------------------------------------------------------

def _state(lifecycle, result=None, message=None):
    lc = SimpleNamespace(value=lifecycle) if lifecycle else None
    rs = SimpleNamespace(value=result) if result else None
    return SimpleNamespace(life_cycle_state=lc, result_state=rs, state_message=message)


def _task(key, lifecycle):
    return SimpleNamespace(
        task_key=key,
        state=SimpleNamespace(life_cycle_state=SimpleNamespace(value=lifecycle)),
    )


def _run(lifecycle, result=None, message=None, tasks=None):
    return SimpleNamespace(state=_state(lifecycle, result, message), tasks=tasks)


# ---------------------------------------------------------------------------
# is_terminal flag
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lifecycle", ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"])
def test_terminal_states_return_is_terminal_true(lifecycle):
    _, is_terminal = format_run_status(_run(lifecycle))
    assert is_terminal is True


@pytest.mark.parametrize("lifecycle", ["PENDING", "RUNNING", "BLOCKED", "WAITING_FOR_RETRY"])
def test_non_terminal_states_return_is_terminal_false(lifecycle):
    _, is_terminal = format_run_status(_run(lifecycle))
    assert is_terminal is False


# ---------------------------------------------------------------------------
# status_text — non-terminal
# ---------------------------------------------------------------------------

def test_running_with_no_tasks():
    text, _ = format_run_status(_run("RUNNING"))
    assert text == "Job RUNNING"


def test_running_with_one_task():
    text, _ = format_run_status(_run("RUNNING", tasks=[_task("ingest", "RUNNING")]))
    assert text == "Job RUNNING | 🔄 ingest: RUNNING"


def test_running_with_multiple_tasks():
    text, _ = format_run_status(_run("RUNNING", tasks=[
        _task("ingest", "RUNNING"),
        _task("transform", "PENDING"),
    ]))
    assert "🔄 ingest: RUNNING" in text
    assert "⏳ transform: PENDING" in text


# ---------------------------------------------------------------------------
# status_text — terminal
# ---------------------------------------------------------------------------

def test_terminated_uses_result_state_when_present():
    text, _ = format_run_status(_run("TERMINATED", result="SUCCESS"))
    assert text == "Job SUCCESS"


def test_terminal_falls_back_to_lifecycle_when_no_result_state():
    text, _ = format_run_status(_run("INTERNAL_ERROR"))
    assert text == "Job INTERNAL_ERROR"


def test_terminal_includes_state_message():
    text, _ = format_run_status(_run("TERMINATED", result="SUCCESS", message="All done"))
    assert text == "Job SUCCESS — All done"


def test_terminal_with_tasks():
    text, _ = format_run_status(_run("TERMINATED", result="SUCCESS", tasks=[
        _task("ingest", "TERMINATED"),
    ]))
    assert text == "Job SUCCESS | ✅ ingest: TERMINATED"


def test_skipped_is_terminal_with_correct_text():
    text, is_terminal = format_run_status(_run("SKIPPED"))
    assert is_terminal is True
    assert text == "Job SKIPPED"


# ---------------------------------------------------------------------------
# Defensive / edge cases
# ---------------------------------------------------------------------------

def test_none_state_defaults_to_pending():
    run = SimpleNamespace(state=None, tasks=None)
    text, is_terminal = format_run_status(run)
    assert text == "Job PENDING"
    assert is_terminal is False


def test_none_life_cycle_state_defaults_to_pending():
    run = SimpleNamespace(
        state=SimpleNamespace(life_cycle_state=None, result_state=None, state_message=None),
        tasks=None,
    )
    text, is_terminal = format_run_status(run)
    assert text == "Job PENDING"
    assert is_terminal is False


def test_none_tasks_does_not_raise():
    text, _ = format_run_status(_run("RUNNING", tasks=None))
    assert text == "Job RUNNING"


def test_unknown_task_state_uses_raw_value_as_label():
    run = _run("RUNNING", tasks=[_task("load", "SOME_FUTURE_STATE")])
    text, _ = format_run_status(run)
    assert "SOME_FUTURE_STATE load: SOME_FUTURE_STATE" in text
