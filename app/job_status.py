TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}

STATE_LABELS = {
    "PENDING": "⏳",
    "RUNNING": "🔄",
    "TERMINATED": "✅",
    "SKIPPED": "⏭",
    "INTERNAL_ERROR": "❌",
    "BLOCKED": "🔒",
    "WAITING_FOR_RETRY": "🔁",
}


def format_run_status(run) -> tuple[str, bool]:
    """
    Summarise a Databricks job run into a display string and a terminal flag.

    Returns:
        status_text: human-readable status line
        is_terminal: True when the run has reached a final state
    """
    overall = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "PENDING"

    task_lines = []
    for t in (run.tasks or []):
        state = t.state.life_cycle_state.value if t.state and t.state.life_cycle_state else "PENDING"
        task_lines.append(f"{STATE_LABELS.get(state, state)} {t.task_key}: {state}")

    task_suffix = "  ·  ".join(task_lines)

    if overall in TERMINAL_STATES:
        result = run.state.result_state.value if run.state and run.state.result_state else ""
        summary = f"Job {result or overall}"
        if run.state and run.state.state_message:
            summary += f" — {run.state.state_message}"
        status_text = summary + (f" | {task_suffix}" if task_suffix else "")
        return status_text, True

    status_text = f"Job {overall}" + (f" | {task_suffix}" if task_suffix else "")
    return status_text, False
