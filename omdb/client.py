import logging

import requests

logger = logging.getLogger(__name__)

OMDB_API_URL = "https://www.omdbapi.com/"

class OmdbMovie:
    """A simple class to represent movie data coming back from OMDb
    and transform to Python types."""

    def __init__(self, data):
        """Data is the raw JSON/dict returned from OMDb"""
        self.data = data

    def check_for_detail_data_key(self, key):
        """Some keys are only in the detail response, raise an
        exception if the key is not found."""
        
        if key not in self.data:
            raise AttributeError(
                f"{key} is not in data, please make sure this is a detail response."
            )

    @property
    def imdb_id(self):
        return self.data["imdbID"]

    @property
    def title(self):
        return self.data["Title"]

    @property
    def year(self):
        return int(self.data["Year"])

    @property
    def runtime_minutes(self):
        self.check_for_detail_data_key("Runtime")

        rt, units = self.data["Runtime"].split(" ")

        if units != "min":
            raise ValueError(f"Expected units 'min' for runtime. Got '{units}")

        return int(rt)

    @property
    def genres(self):
        self.check_for_detail_data_key("Genre")

        return self.data["Genre"].split(", ")

    @property
    def plot(self):
        self.check_for_detail_data_key("Plot")
        return self.data["Plot"]

class OmdbClient:
    """A client to interact with the OMDb API."""

    def __init__(self, api_key):
        self.api_key = api_key

    def _make_request(self, params):
        """Make a request to the OMDb API and return the response."""
        params["apikey"] = self.api_key
        logger.debug(f"Making request to OMDb API with params: {params}")
        response = requests.get(OMDB_API_URL, params=params)

        if response.status_code != 200:
            logger.error(
                f"OMDb API request failed with status code {response.status_code}"
            )
            response.raise_for_status()

        data = response.json()

        if data.get("Response") == "False":
            logger.warning(f"OMDb API error: {data.get('Error')}")
            raise ValueError(f"OMDb API error: {data.get('Error')}")

        return data

    def search(self, title, movie_type="movie", page=1):
        """Search for movies by title."""
        logger.info(f"Searching for movies with title '{title}'")
        params = {"s": title, "type": movie_type, "page": page}
        return self._make_request(params)

    def get_movie_details(self, imdb_id):
        """Get detailed information about a movie by its IMDb ID."""
        logger.info(f"Fetching details for movie with IMDb ID '{imdb_id}'")
        params = {"i": imdb_id, "plot": "full"}
        return self._make_request(params)