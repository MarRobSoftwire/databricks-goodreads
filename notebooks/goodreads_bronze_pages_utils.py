def assert_authenticated(html: str, book_id: str) -> None:
    """Raise RuntimeError if the fetched HTML does not contain the expected book ID."""
    if book_id not in html:
        print(f"[{book_id}] Auth failed — HTML preview: {html[:500].strip()!r}")
        raise RuntimeError(
            f"Book ID '{book_id}' not found in response. "
            "Cookie may have expired — update the Databricks secret and re-run."
        )
