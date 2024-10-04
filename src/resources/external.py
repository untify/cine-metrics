from contextlib import asynccontextmanager
from typing import Dict, Optional

import aiohttp
from dagster import ConfigurableResource


class APILimitReachedException(Exception):
    """Exception indicating that the API limit has been reached."""

    pass


class OMDbAPIResource(ConfigurableResource):
    api_key: str
    base_url: str = "http://www.omdbapi.com/"
    timeout: int = 10

    async def fetch_movie_data(
        self, context, session: aiohttp.ClientSession, title: str
    ) -> Optional[Dict]:
        params = {"apikey": self.api_key, "t": title, "plot": "full"}
        try:
            async with session.get(self.base_url, params=params, timeout=self.timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("Response") == "True":
                        return data
                    elif data.get("Error") == "Request limit reached!":
                        context.log.info("API request limit reached.")
                        raise APILimitReachedException("API request limit reached.")
                    else:
                        context.log.warning(f"No data found for movie: {title}")
                        return None
                elif response.status == 401:
                    context.log.info("API key is invalid or daily limit has been reached.")
                    raise APILimitReachedException(
                        "API key is invalid or daily limit has been reached."
                    )
                else:
                    context.log.error(f"Error fetching data for {title}: HTTP {response.status}")
                    return None
        except APILimitReachedException:
            raise
        except aiohttp.ClientError as e:
            context.log.error(f"Network error fetching data for {title}: {str(e)}")
            return None
        except Exception as e:
            context.log.error(f"Unexpected error fetching data for {title}: {str(e)}")
            return None

    @asynccontextmanager
    async def get_session(self):
        async with aiohttp.ClientSession() as session:
            yield session
