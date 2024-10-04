import asyncio
import json
from datetime import date
from typing import Any, Dict, List, NamedTuple, Tuple

import aiohttp
from dagster import AssetExecutionContext

from src.resources.external import APILimitReachedException


class MovieToFetch(NamedTuple):
    clean_title: str
    max_date: date


def initialize_tables(database):
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_omdb_raw_data (
            imdb_id VARCHAR PRIMARY KEY,
            data JSON,
            last_updated DATE
        )
    """
    )
    database.execute(
        """
        CREATE TABLE IF NOT EXISTS stg_omdb_api_usage_log (
            date DATE PRIMARY KEY,
            request_count INTEGER
        )
    """
    )


def insert_omdb_data(database, imdb_id: str, data: Dict[str, Any], current_date: date):
    database.execute(
        """
        INSERT INTO stg_omdb_raw_data (imdb_id, data, last_updated)
        VALUES (?, ?, ?)
        ON CONFLICT (imdb_id) DO UPDATE SET
        data = EXCLUDED.data,
        last_updated = EXCLUDED.last_updated
    """,
        [imdb_id, json.dumps(data), current_date],
    )


def update_api_usage_log(database, current_date: date, request_count: int):
    database.execute(
        """
        INSERT INTO stg_omdb_api_usage_log (date, request_count)
        VALUES (?, ?)
        ON CONFLICT (date) DO UPDATE SET
        request_count = stg_omdb_api_usage_log.request_count + EXCLUDED.request_count
    """,
        [current_date, request_count],
    )


def update_fetch_date(database, clean_title: str, current_date: date):
    database.execute(
        """
        UPDATE stg_movies_to_fetch
        SET omdb_last_fetched_date = ?
        WHERE clean_title = ?
    """,
        [current_date, clean_title],
    )


def update_error_date(database, clean_title: str, current_date: date):
    database.execute(
        """
        UPDATE stg_movies_to_fetch
        SET omdb_last_error_date = ?
        WHERE clean_title = ?
    """,
        [current_date, clean_title],
    )


def get_titles_to_fetch(database, seven_days_ago: date) -> List[MovieToFetch]:
    results = database.query(
        """
        SELECT clean_title, max_date
        FROM stg_movies_to_fetch
        WHERE (omdb_last_fetched_date IS NULL AND omdb_last_error_date IS NULL)
           OR (omdb_last_fetched_date < ? AND (omdb_last_error_date IS NULL OR omdb_last_error_date < ?))
           OR (omdb_last_fetched_date IS NULL AND omdb_last_error_date < ?)
        ORDER BY max_date DESC
    """,
        [seven_days_ago, seven_days_ago, seven_days_ago],
    )
    return [MovieToFetch(*row) for row in results]


async def process_single_movie(
    context: AssetExecutionContext,
    omdb_api,
    database,
    session,
    movie: MovieToFetch,
    current_date: date,
) -> Tuple[bool, int]:
    try:
        result = await omdb_api.fetch_movie_data(context, session, movie.clean_title)
        if result:
            await database.run_async(
                insert_omdb_data, database, result["imdbID"], result, current_date
            )
            await database.run_async(update_fetch_date, database, movie.clean_title, current_date)
            return True, 1
        else:
            await database.run_async(update_error_date, database, movie.clean_title, current_date)
            return False, 1
    except APILimitReachedException:
        raise
    except aiohttp.ClientError as e:
        context.log.error(f"Network error processing movie {movie.clean_title}: {str(e)}")
    except Exception as e:
        context.log.error(f"Unexpected error processing movie {movie.clean_title}: {str(e)}")

    await database.run_async(update_error_date, database, movie.clean_title, current_date)
    return False, 1


async def process_movies(
    context: AssetExecutionContext,
    omdb_api,
    database,
    session,
    titles_to_fetch: List[MovieToFetch],
    current_date: date,
) -> Tuple[int, int]:
    semaphore = asyncio.Semaphore(10)
    processed_count = 0
    total_requests = 0
    fetch_tasks = []

    async def process_with_semaphore(movie):
        nonlocal processed_count, total_requests
        async with semaphore:
            success, requests = await process_single_movie(
                context, omdb_api, database, session, movie, current_date
            )
            if success:
                processed_count += 1
            total_requests += requests
            return success, requests

    for movie in titles_to_fetch:
        task = asyncio.create_task(process_with_semaphore(movie))
        fetch_tasks.append(task)

    try:
        await asyncio.gather(*fetch_tasks)
    except APILimitReachedException as e:
        context.log.warning(f"API Limit reached during processing: {str(e)}")
        for task in fetch_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*fetch_tasks, return_exceptions=True)

    return processed_count, total_requests
