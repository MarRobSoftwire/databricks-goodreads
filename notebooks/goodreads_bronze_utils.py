import xml.etree.ElementTree as ET


def parse_rss_items(rss_bytes: bytes) -> list:
    """Parse Goodreads RSS XML bytes into a list of book dicts with all fields as strings."""
    root = ET.fromstring(rss_bytes)
    channel = root.find("channel")

    books = []
    for item in channel.findall("item"):

        def text(tag, _item=item):
            el = _item.find(tag)
            return (el.text or "").strip() if el is not None else ""

        books.append({
            "title":            text("title"),
            "author":           text("author_name"),
            "isbn":             text("isbn"),
            "book_id":          text("book_id"),
            "num_pages":        item.findtext("book/num_pages", default=""),
            "year_published":   text("book_published"),
            "average_rating":   text("average_rating"),
            "user_rating":      text("user_rating"),
            "read_at":          text("user_read_at"),
            "date_added":       text("user_date_added"),
            "shelves":          text("user_shelves"),
            "review":           text("user_review"),
            "book_description": text("book_description"),
            "cover_url":        text("book_medium_image_url"),
            "goodreads_url":    text("link"),
        })

    return books
