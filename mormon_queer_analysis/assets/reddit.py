from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Literal

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
)
import pandas as pd
import requests

from mormon_queer_analysis.partitions import reddit_partitions
from mormon_queer_analysis.resources import FILTER_KEYWORDS


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
    partition_date_str = context.partition_key.keys_by_dimension["date"]
    partition_subreddit = context.partition_key.keys_by_dimension["subreddit"]

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

    return df


@asset(partitions_def=reddit_partitions)
def raw_reddit_posts(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Retrieves raw Reddit posts from the arctic shift API.
    """

    df = raw_reddit_data(context, reddit_data_type="posts")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset(partitions_def=reddit_partitions)
def raw_reddit_comments(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Retrieves raw Reddit comments from the arctic shift API.
    """

    df = raw_reddit_data(context, reddit_data_type="comments")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset(partitions_def=reddit_partitions)
def topical_reddit_posts(
    context: AssetExecutionContext,
    raw_reddit_posts: pd.DataFrame,
) -> pd.DataFrame:
    """
    Filters reddit posts to ones related to the topic, based on if they
    contain relevant keywords. Formats data to keep only necessary
    columns, and renames them to be less reddit-specific.
    """

    # If there was no upstream content, return an empty dataframe
    if raw_reddit_posts.empty:
        return pd.DataFrame()

    # Concatenate the post's title and body into a new column 'text'
    raw_reddit_posts["text"] = (
        raw_reddit_posts["title"] + "\n" + raw_reddit_posts["selftext"]
    )

    # Filter posts based on keywords
    keyword_filter = raw_reddit_posts["text"].apply(
        lambda x: any(keyword.lower() in x.lower() for keyword in FILTER_KEYWORDS)
    )
    filtered_df = raw_reddit_posts[keyword_filter]

    # Keep only necessary columns, and rename them to be less reddit-specific
    filtered_df = filtered_df[["created_utc", "score", "name", "text"]].copy()
    filtered_df.rename(columns={"created_utc": "date"}, inplace=True)

    context.add_output_metadata(
        metadata={
            "num_records": len(
                filtered_df
            ),  # TODO: Add an EventMetadataEntry when num_records is 0 to warn if the dataframe was empty
            "preview": MetadataValue.md(filtered_df.head().to_markdown()),
        }
    )

    return filtered_df


@asset(partitions_def=reddit_partitions)
def topical_reddit_comments(
    context: AssetExecutionContext,
    raw_reddit_comments: pd.DataFrame,
    topical_reddit_posts: pd.DataFrame,
) -> pd.DataFrame:
    """
    Filters reddit comments to ones related to the topic, based on if
    they contain relevant keywords or were commented on relevant posts.
    Formats data to keep only necessary columns, and renames them to be
    less reddit-specific.
    """

    # If there was no upstream content, return an empty dataframe
    if raw_reddit_comments.empty:
        return pd.DataFrame()

    # Filter comments based on if they contain related keywords, or they were commented on a post with related keywords
    related_post_ids = set(topical_reddit_posts["name"].unique())
    related_post_filter = raw_reddit_comments["link_id"].apply(
        lambda x: x in related_post_ids
    )
    keyword_filter = raw_reddit_comments["body"].apply(
        lambda x: any(keyword.lower() in x.lower() for keyword in FILTER_KEYWORDS)
    )
    filtered_comments_df = raw_reddit_comments[related_post_filter | keyword_filter]

    # Keep only necessary columns, and rename them to be less reddit-specific
    filtered_comments_df = filtered_comments_df[["created_utc", "score", "body"]].copy()
    filtered_comments_df.rename(
        columns={"created_utc": "date", "body": "text"}, inplace=True
    )

    context.add_output_metadata(
        metadata={
            "num_records": len(filtered_comments_df),
            "preview": MetadataValue.md(filtered_comments_df.head().to_markdown()),
        }
    )

    return filtered_comments_df
