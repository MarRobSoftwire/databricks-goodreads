def subject_to_genres(subjects: list | None) -> list[str]:
    """Map a list of Open Library subjects to a deduplicated list of genre labels.

    Matching uses substring containment where the keyword is specific enough to
    avoid false positives (e.g. 'science fiction' never matches bare 'fiction').
    Bare 'fiction' is never assigned a genre.

    Args:
        subjects: list of subject strings from goodreads.silver_open_library.

    Returns:
        Sorted list of matched genre labels. A single subject can match multiple
        genres (e.g. 'science fiction & fantasy' → ['Fantasy', 'Science Fiction']).
    """
    genres: set[str] = set()

    for raw in (subjects or []):
        s = raw.lower().strip()

        if "science fiction" in s or "sci-fi" in s or "science-fiction" in s:
            genres.add("Science Fiction")

        if "fantasy" in s:
            genres.add("Fantasy")

        if "historical fiction" in s or "fiction, historical" in s or s == "historical":
            genres.add("Historical Fiction")

        if "mystery" in s or "detective" in s or "misterio" in s or s in ("suspense fiction",
                                                                           "fiction, suspense",
                                                                           "roman policier"):
            genres.add("Mystery & Crime")

        if "thriller" in s or s in ("suspense & thriller", "thrillers & suspense",
                                     "novela de suspense"):
            genres.add("Thriller")

        if "romance" in s or s in ("love stories", "love & romance"):
            genres.add("Romance")

        if "dystop" in s or s in ("post-apocalyptic fiction", "apocalyptic fiction"):
            genres.add("Dystopian")

        if "young adult" in s or "young adult" in s or s in ("ya", "teen fiction",
                                                                      "teenage literature"):
            genres.add("Young Adult")

        if "literary" in s or "classic" in s:
            genres.add("Literary Fiction")

        if "graphic novel" in s or "comic" in s:
            genres.add("Graphic Novel")

        if "lgbt" in s or s.startswith("gay") or " gay " in s:
            genres.add("LGBTQ+")

        if "adventure" in s or s in ("action & adventure", "action/adventure",
                                      "action and adventure fiction"):
            genres.add("Action & Adventure")

    return sorted(genres)
