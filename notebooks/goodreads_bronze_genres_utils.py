import json
import urllib.parse
import urllib.request

BASE_URL = "https://openlibrary.org/api/books?bibkeys={bibkeys}&format=json&jscmd=data"


def fetch_batch(batch: list, base_url: str = BASE_URL) -> tuple[str, str, dict]:
    """Fetch one batch of ISBN rows from the Open Library Books API.

    Args:
        batch: list of Row objects with `.isbn` and `.book_id` attributes.
        base_url: API URL template with a `{bibkeys}` placeholder.

    Returns:
        (bibkeys_str, api_url, parsed_json_dict)
    """
    bibkeys = ",".join(f"ISBN:{row.isbn}" for row in batch)
    url = base_url.format(bibkeys=urllib.parse.quote(bibkeys, safe=",:/"))
    req = urllib.request.Request(url, headers={"User-Agent": "goodreads-books-app (contact@example.org)"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return bibkeys, url, json.loads(resp.read().decode("utf-8"))
