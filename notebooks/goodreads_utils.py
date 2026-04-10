from dateutil import parser as dateutil_parser


def parse_date(raw: str):
    """Parse a date string into a Python date, returning None for empty/None input."""
    if not raw:
        return None
    return dateutil_parser.parse(raw).date()
