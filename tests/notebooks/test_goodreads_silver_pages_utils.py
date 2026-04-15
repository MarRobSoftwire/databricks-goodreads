import pytest
from goodreads_silver_pages_utils import extract_start_date_str


def _timeline_div(date: str, label: str) -> str:
    """Build a single readingTimeline__text div in the real Goodreads format: '<date> – <label>'."""
    return f'<div class="readingTimeline__text">{date} – {label}</div>'


def _wrap(*divs: str) -> str:
    return f"<html><body>{''.join(divs)}</body></html>"


def test_returns_none_for_none_input():
    assert extract_start_date_str(None) is None


def test_returns_none_for_empty_string():
    assert extract_start_date_str("") is None


def test_returns_none_when_no_timeline_divs_present():
    html = _wrap("<p>No timeline here</p>")

    assert extract_start_date_str(html) is None


def test_returns_none_when_only_finished_reading_present():
    html = _wrap(_timeline_div("April 10, 2026", "Finished Reading"))

    assert extract_start_date_str(html) is None


def test_returns_none_when_started_reading_has_no_dash():
    html = _wrap('<div class="readingTimeline__text">March 29, 2026 Started Reading</div>')

    assert extract_start_date_str(html) is None

def test_returns_none_when_started_reading_has_wrong_dash():
    html = _wrap('<div class="readingTimeline__text">March 29, 2026 - Started Reading</div>')

    assert extract_start_date_str(html) is None


def test_returns_none_for_unrelated_shelf_entry():
    html = _wrap(_timeline_div("January 1, 2024", "Shelved"))

    assert extract_start_date_str(html) is None


def test_extracts_date_from_single_started_reading_entry():
    html = _wrap(_timeline_div("March 29, 2026", "Started Reading"))

    assert extract_start_date_str(html) == "March 29, 2026"


def test_ignores_finished_reading_and_returns_start_date():
    html = _wrap(
        _timeline_div("March 1, 2026",  "Started Reading"),
        _timeline_div("March 29, 2026", "Finished Reading"),
    )

    assert extract_start_date_str(html) == "March 1, 2026"


def test_uses_last_started_reading_when_book_read_multiple_times():
    html = _wrap(
        _timeline_div("January 1, 2024",  "Started Reading"),
        _timeline_div("January 20, 2024", "Finished Reading"),
        _timeline_div("March 1, 2026",    "Started Reading"),
        _timeline_div("March 29, 2026",   "Finished Reading"),
    )

    assert extract_start_date_str(html) == "March 1, 2026"


def test_does_not_return_first_read_when_reread():
    html = _wrap(
        _timeline_div("January 1, 2024", "Started Reading"),
        _timeline_div("March 1, 2026",   "Started Reading"),
    )

    assert extract_start_date_str(html) != "January 1, 2024"
