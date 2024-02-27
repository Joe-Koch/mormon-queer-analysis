from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal

from dagster import (
    AssetExecutionContext,
    asset,
)
import pandas as pd
import requests

from mormon_queer_analysis.partitions import reddit_partitions
from mormon_queer_analysis.resources import FILTER_KEYWORDS
from mormon_queer_analysis.resources.duckdb_io_manager import Database


def raw_reddit_data(
    context: AssetExecutionContext, reddit_data_type: Literal["comments", "posts"]
) -> pd.DataFrame:
    """
    Fetches raw data from the arctic shift reddit data dump API for a
    specified type of Reddit content (either comments or posts) in the
    specified subreddit. The data fetched is based on a specific
    partition date provided by the Dagster context to filter the data to
    only include content from that month.

    Raises:
    - HTTPError: If the request to the arctic shift API fails.
    - JSONDecodeError: If the response from the API is not valid JSON.
    """

    base_url = f"https://arctic-shift.photon-reddit.com/api/{reddit_data_type}/search"

    partition = context.partition_key.keys_by_dimension
    partition_date_str = partition["date"]
    partition_subreddit = partition["subreddit"]

    filters = {
        "subreddit": partition_subreddit,
        "after": partition_date_str,
        "before": (
            datetime.strptime(partition_date_str, "%Y-%m-%d") + relativedelta(months=1)
        ).strftime("%Y-%m-%d"),
        "limit": "100",
    }
    query_params = "&".join([f"{key}={value}" for key, value in filters.items()])
    url = f"{base_url}?{query_params}"

    results = requests.get(url).json()

    df = pd.DataFrame(results["data"])

    desired_columns = {
        "posts": [
            "author",
            "permalink",
            "title",
            "name",
            "selftext",
            "created_utc",
            "score",
        ],
        "comments": [
            "author",
            "permalink",
            "link_id",
            "body",
            "created_utc",
            "score",
        ],
    }

    df2 = df[desired_columns[reddit_data_type]]
    # TODO: use dbt to clean up schemas
    df2.rename(columns={"created_utc": "date"}, inplace=True)
    pd.to_datetime(df2["date"], unit="s")
    df2["subreddit"] = partition_subreddit

    if reddit_data_type == "posts":
        df2["title"] = df2["title"].astype("string")
        df2["selftext"] = df2["selftext"].astype("string")

    return df2


@asset(
    partitions_def=reddit_partitions,
    metadata={
        "partition_expr": {"date": "TO_TIMESTAMP(date)", "subreddit": "subreddit"}
    },
)
def raw_reddit_posts(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Retrieves raw Reddit posts from the arctic shift API.
    """

    df = raw_reddit_data(context, reddit_data_type="posts")

    return df


@asset(
    partitions_def=reddit_partitions,
    metadata={
        "partition_expr": {"date": "TO_TIMESTAMP(date)", "subreddit": "subreddit"}
    },
)
def raw_reddit_comments(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Retrieves raw Reddit comments from the arctic shift API.
    """

    df = raw_reddit_data(context, reddit_data_type="comments")

    return df


@asset(
    deps=[raw_reddit_posts, raw_reddit_comments],
)
def topical_reddit_posts_and_comments(database: Database) -> pd.DataFrame:
    """
    Filters reddit posts and comments to ones related to the topic, based on if
    they contain relevant keywords or were commented on relevant posts.
    Formats data to keep only necessary columns, and renames them to be
    less reddit-specific.
    """

    keywords_condition = " OR ".join(
        [f"LOWER(text) LIKE '%{keyword.lower()}%'" for keyword in FILTER_KEYWORDS]
    )
    # Filter posts based on if they contain related keywords, and comments based on if they contain related keywords, or they were commented on a post with related keywords
    filtered_df = database.query(
        f"""
        WITH topical_reddit_posts AS (
            SELECT
                date,
                score,
                title || '\n' || selftext AS text,
                subreddit,
                name
            FROM
                REDDIT.raw_reddit_posts
            WHERE
                {keywords_condition}
        )

        SELECT
            c.date,
            c.score,
            c.body AS text,
            c.subreddit
        FROM
            REDDIT.raw_reddit_comments c
        WHERE
            c.link_id IN (SELECT name FROM topical_reddit_posts)
            OR
            {keywords_condition}

        UNION ALL

        SELECT
            p.date,
            p.score,
            p.text,
            p.subreddit
        FROM
            topical_reddit_posts p

    """
    )

    return filtered_df
