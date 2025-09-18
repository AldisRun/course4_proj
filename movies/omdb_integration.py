import logging
import re
import requests
from datetime import timedelta

from django.utils.timezone import now
from django.conf import settings

from movies.models import Genre, SearchTerm, Movie
from omdb.django_client import get_client_from_settings

logger = logging.getLogger(__name__)

def _fetch_by_imdb_id(omdb_client, imdb_id):
    """
    Try common client method names; if none exist, fall back to direct HTTP GET to OMDb.
    Requires OMDB_API_KEY (or OMDB_APIKEY/OMDB_KEY) in Django settings for the HTTP fallback.
    """
    # Try various client methods with different signatures
    for name in ("get_by_imdb_id", "by_id", "get_movie", "movie", "id", "lookup", "get"):
        getter = getattr(omdb_client, name, None)
        if not callable(getter):
            continue
        # Try positional then keyword forms
        try:
            return getter(imdb_id)            # e.g. by_id("tt1853728")
        except TypeError:
            try:
                return getter(imdbid=imdb_id) # e.g. get(imdbid="tt1853728")
            except TypeError:
                try:
                    return getter(id=imdb_id) # some APIs use id=...
                except Exception:
                    continue
        except Exception:
            continue

    # ---- HTTP fallback ----
    api_key = (
        getattr(settings, "OMDB_API_KEY", None)
        or getattr(settings, "OMDB_APIKEY", None)
        or getattr(settings, "OMDB_KEY", None)
    )
    if not api_key:
        logger.error("OMDb client lacks a by-ID method and no OMDB_API_KEY found in settings for HTTP fallback.")
        return None

    params = {"i": imdb_id, "plot": "full", "apikey": api_key}
    try:
        resp = requests.get("https://www.omdbapi.com/", params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("Response") == "True":
            return data
        logger.error("OMDb responded with error for %s: %s", imdb_id, data.get("Error"))
        return None
    except Exception as e:
        logger.exception("HTTP fallback to OMDb failed for %s: %s", imdb_id, e)
        return None

def get_or_create_genres(genre_names):
    for genre_name in genre_names or []:
        genre, _ = Genre.objects.get_or_create(name=genre_name)
        yield genre


def _callable_attr(obj, name):
    """Get attribute and call it if it’s callable; else return as-is."""
    v = getattr(obj, name, None)
    return v() if callable(v) else v


def _normalize_detail_obj(detail):
    """
    Normalize a detail payload (dict or object or str) into a dict:
    {Title, Year, imdbID, Plot, RuntimeMinutes, Genres[list]}
    """
    if detail is None:
        return {}

    # Dict (common for python-omdb)
    if isinstance(detail, dict):
        title = detail.get("Title")
        year = detail.get("Year")
        imdb_id = detail.get("imdbID") or detail.get("imdbId") or detail.get("imdb_id")
        plot = detail.get("Plot")
        runtime_str = detail.get("Runtime") or detail.get("runtime")
        genres = detail.get("Genre") or detail.get("genres")

    # Non-string object with attributes
    elif not isinstance(detail, str):
        title = _callable_attr(detail, "title")
        year = _callable_attr(detail, "year")
        imdb_id = _callable_attr(detail, "imdb_id") or _callable_attr(detail, "imdbID")
        plot = _callable_attr(detail, "plot")
        runtime_str = _callable_attr(detail, "runtime") or _callable_attr(detail, "runtime_minutes")
        genres = _callable_attr(detail, "genres")

    # Plain string → only a title text
    else:
        title = detail
        year = None
        imdb_id = None
        plot = None
        runtime_str = None
        genres = None

    # Normalize year -> int when possible (handles "1999–")
    try:
        if isinstance(year, int):
            year_val = year
        elif isinstance(year, str) and year.isdigit():
            year_val = int(year)
        else:
            m = re.match(r"^\d{4}", str(year)) if year is not None else None
            year_val = int(m.group(0)) if m else None
    except Exception:
        year_val = None

    # Normalize runtime minutes
    runtime_minutes = None
    try:
        if isinstance(runtime_str, int):
            runtime_minutes = runtime_str
        elif isinstance(runtime_str, str):
            m = re.search(r"(\d+)", runtime_str)
            runtime_minutes = int(m.group(1)) if m else None
    except Exception:
        pass

    # Normalize genres -> list[str]
    if isinstance(genres, str):
        genre_list = [g.strip() for g in genres.split(",") if g.strip()]
    elif isinstance(genres, (list, tuple)):
        genre_list = [str(g).strip() for g in genres if str(g).strip()]
    else:
        genre_list = []

    return {
        "Title": title,
        "Year": year_val,
        "imdbID": imdb_id,
        "Plot": plot,
        "RuntimeMinutes": runtime_minutes,
        "Genres": genre_list,
    }


def fill_movie_details(movie):
    """
    Fetch a movie's full details from OMDb. Then, save it to the DB.
    If the movie already has a `full_record` this does nothing.
    """
    if movie.is_full_record:
        logger.warning("'%s' is already a full record.", movie.title)
        return

    omdb_client = get_client_from_settings()

    details_raw = _fetch_by_imdb_id(omdb_client, movie.imdb_id)
    if not details_raw:
        logger.error("Unable to fetch details for IMDb ID '%s'.", movie.imdb_id)
        return

    details = _normalize_detail_obj(details_raw)

    movie.title = details.get("Title") or movie.title
    movie.year = details.get("Year") or movie.year
    movie.plot = details.get("Plot")
    movie.runtime_minutes = details.get("RuntimeMinutes")

    movie.genres.clear()
    for genre in get_or_create_genres(details.get("Genres", [])):
        movie.genres.add(genre)

    movie.is_full_record = True
    movie.save()
    logger.info("Movie '%s' updated with full details.", movie.title)


def _normalize_search_item(item, omdb_client):
    """
    Normalize one item from search() into {'Title','Year','imdbID','Type'}.
    Handle str first, then dict, then non-str objects.
    """
    # 1) Plain string → try to resolve via title; fall back to bare title
    if isinstance(item, str):
        for getter_name in ("get", "get_by_title", "title", "by_title"):
            getter = getattr(omdb_client, getter_name, None)
            try:
                if callable(getter):
                    res = getter(title=item) if getter_name == "get" else getter(item)
                    if res:
                        det = _normalize_detail_obj(res)
                        return {
                            "Title": det.get("Title") or item,
                            "Year": det.get("Year"),
                            "imdbID": det.get("imdbID"),
                            "Type": (res.get("Type") if isinstance(res, dict) else getattr(res, "type", None)),
                        }
            except TypeError:
                continue
            except Exception as e:
                logger.warning("Title lookup failed for '%s': %s", item, e)
                break
        return {"Title": item, "Year": None, "imdbID": None, "Type": None}

    # 2) Dict from python-omdb
    if isinstance(item, dict):
        return {
            "Title": item.get("Title"),
            "Year": item.get("Year"),
            "imdbID": item.get("imdbID") or item.get("imdb_id") or item.get("imdbId"),
            "Type": item.get("Type"),
        }

    # 3) Non-str object with attributes
    return {
        "Title": _callable_attr(item, "title"),
        "Year": _callable_attr(item, "year"),
        "imdbID": _callable_attr(item, "imdb_id") or _callable_attr(item, "imdbID"),
        "Type": _callable_attr(item, "type"),
    }


def _should_override_cache_guard():
    """
    In dev/tests it’s convenient to re-hit the API without waiting 24h.
    Override when DEBUG=True or OMDB_ALLOW_RESCRAPE=True in settings.
    """
    return getattr(settings, "OMDB_ALLOW_RESCRAPE", False) or getattr(settings, "DEBUG", False)


def search_and_save(search):
    """
    Perform a search for `search` against the API, but only if it hasn't been searched in the past 24 hours.
    Save each result to the local DB as a partial record.
    """
    # Replace multiple spaces with single spaces, and lowercase the search
    normalized_search_term = re.sub(r"\s+", " ", search.lower())

    search_term, created = SearchTerm.objects.get_or_create(term=normalized_search_term)

    recent = (not created) and (search_term.last_search and search_term.last_search > now() - timedelta(days=1))
    if recent and not _should_override_cache_guard():
        logger.warning(
            "Search for '%s' was performed in the past 24 hours so not searching again.",
            normalized_search_term,
        )
        return
    elif recent and _should_override_cache_guard():
        logger.warning(
            "Search for '%s' was performed in the past 24 hours — overriding due to DEBUG/OMDB_ALLOW_RESCRAPE.",
            normalized_search_term,
        )

    omdb_client = get_client_from_settings()
    logger.info("Performing a search for '%s'", normalized_search_term)

    results = omdb_client.search(search) or []

    # Unwrap OMDb envelope: {"Search": [...], "totalResults": "...", "Response": "True"}
    if isinstance(results, dict):
        inner = results.get("Search") or results.get("search") or results.get("results")
        if inner is None:
            logger.warning("OMDb returned a dict without a 'Search' array: %r", results)
            results = []
        else:
            logger.debug("Extracted %d items from OMDb 'Search' array.", len(inner))
            results = inner

    if not results:
        logger.info("No results returned for '%s'.", normalized_search_term)

    for raw in results:
        data = _normalize_search_item(raw, omdb_client)
        if not data:
            continue

        # Only movies (skip 'series'/'episode' if Type present)
        if data.get("Type") and data["Type"] != "movie":
            continue

        title = data.get("Title")
        year = data.get("Year")
        imdb_id = data.get("imdbID")

        # Resolve imdbID by title if missing
        if not imdb_id and title:
            resolver = getattr(omdb_client, "get", None)
            if callable(resolver):
                try:
                    resolved = resolver(title=title, year=year) if year else resolver(title=title)
                    details = _normalize_detail_obj(resolved)
                    imdb_id = details.get("imdbID") or imdb_id
                    title = details.get("Title") or title
                    year = details.get("Year") or year
                except Exception as e:
                    logger.warning("Failed to resolve imdbID for '%s': %s", title, e)

        if not imdb_id:
            logger.warning("Skipping item without imdbID: %r", data)
            continue

        # Normalize year -> int
        year_val = None
        try:
            if isinstance(year, int):
                year_val = year
            elif isinstance(year, str) and year.isdigit():
                year_val = int(year)
            elif year is not None:
                m = re.match(r"^\d{4}", str(year))
                year_val = int(m.group(0)) if m else None
        except Exception:
            year_val = None

        logger.info("Saving movie: '%s' / '%s'", title, imdb_id)
        movie, created = Movie.objects.get_or_create(
            imdb_id=imdb_id,
            defaults={
                "title": title,
                "year": year_val,
            },
        )
        if created:
            logger.info("Movie created: '%s'", movie.title)

    # Important: record that we searched now
    search_term.last_search = now()
    search_term.save()