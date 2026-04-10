from bs4 import BeautifulSoup


def extract_start_date_str(raw_html: str):
    """
    Finds the readingTimeline row containing 'Started Reading' and returns the
    raw date string (e.g. 'March 29, 2026'). Returns None if no start date is recorded.
    """
    if not raw_html:
        return None
    soup = BeautifulSoup(raw_html, "html.parser")
    matches = [
        row.get_text(separator=" ", strip=True)
        for row in soup.find_all("div", class_="readingTimeline__text")
        if "Started Reading" in row.get_text() and "–" in row.get_text()
    ]
    if not matches:
        return None
    return matches[-1].split("–")[0].strip()
