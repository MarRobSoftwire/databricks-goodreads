import json


def parse_open_library_record(raw_json: str | None) -> dict:
    """Parse a raw Open Library Books API JSON string into a flat dict.

    Extracts the fields relevant to genre enrichment. Returns a dict with
    None values for any fields absent in the response, so the schema stays
    consistent regardless of how sparse the record is.

    Args:
        raw_json: JSON string as stored in goodreads.bronze_open_library.
                  May be None for rows where the API returned no data.

    Returns:
        dict with keys: ol_key, ol_title, subjects, authors,
        publishers, publish_date, number_of_pages, cover_url.
    """
    empty = {
        "ol_key":          None,
        "ol_title":        None,
        "subjects":        [],
        "authors":         [],
        "publishers":      [],
        "publish_date":    None,
        "number_of_pages": None,
        "cover_url":       None,
    }

    if not raw_json:
        return empty

    try:
        record = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        return empty

    def names(lst: list) -> list[str]:
        """Extract the 'name' field from a list of {name, url} objects."""
        return [item["name"] for item in (lst or []) if isinstance(item, dict) and "name" in item]

    pages = record.get("number_of_pages")
    if pages is None:
        # Fall back to the pagination string field (e.g. "416 p." or "416")
        pagination = record.get("pagination", "")
        digits = "".join(c for c in str(pagination) if c.isdigit())
        pages = int(digits) if digits else None

    cover = record.get("cover") or {}

    return {
        "ol_key":          record.get("key"),
        "ol_title":        record.get("title"),
        "subjects":        names(record.get("subjects")),
        "authors":         names(record.get("authors")),
        "publishers":      names(record.get("publishers")),
        "publish_date":    record.get("publish_date"),
        "number_of_pages": pages,
        "cover_url":       cover.get("large") or cover.get("medium") or cover.get("small"),
    }
