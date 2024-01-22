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

from mormon_queer_analysis.partitions import monthly_partitions


def raw_reddit_data(
    context: AssetExecutionContext, reddit_data_type: Literal["comments", "posts"]
) -> pd.DataFrame:
    base_url = f"https://arctic-shift.photon-reddit.com/api/{reddit_data_type}/search"
    partition_date_str = context.asset_partition_key_for_output()

    filters = {
        "subreddit": "mormon",
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


@asset(partitions_def=monthly_partitions)
def raw_reddit_posts(context: AssetExecutionContext) -> pd.DataFrame:
    df = raw_reddit_data(context, reddit_data_type="posts")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset(partitions_def=monthly_partitions)
def raw_reddit_comments(context: AssetExecutionContext) -> pd.DataFrame:
    df = raw_reddit_data(context, reddit_data_type="comments")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset(partitions_def=monthly_partitions)
def topical_reddit_posts(
    context: AssetExecutionContext,
    raw_reddit_posts: pd.DataFrame,
) -> pd.DataFrame:
    # Concatenate the post's title and body into a new column 'text'
    raw_reddit_posts["text"] = (
        raw_reddit_posts["title"] + "\n" + raw_reddit_posts["selftext"]
    )

    # Filter posts based on keywords
    # with open("../resources/filter_keywords.txt", "r") as file:
    #     filter_keywords = [line.strip() for line in file.readlines() if line.strip()]
    filter_keywords = ("BYU", "General Conference")
    keyword_filter = raw_reddit_posts["text"].apply(
        lambda x: any(keyword.lower() in x.lower() for keyword in filter_keywords)
    )
    filtered_df = raw_reddit_posts[keyword_filter]

    # Keep only necessary columns, and rename them to be less reddit-specific
    filtered_df = filtered_df[["created_utc", "score", "name", "text"]].copy()
    filtered_df.rename(columns={"created_utc": "date"}, inplace=True)

    context.add_output_metadata(
        metadata={
            "num_records": len(filtered_df),
            "preview": MetadataValue.md(filtered_df.head().to_markdown()),
        }
    )

    return filtered_df


@asset(partitions_def=monthly_partitions)
def topical_reddit_comments(
    context: AssetExecutionContext,
    raw_reddit_comments: pd.DataFrame,
    topical_reddit_posts: pd.DataFrame,
) -> pd.DataFrame:
    # Filter comments based on if they contain related keywords, or they were commented on a post with related keywords
    related_post_ids = set(topical_reddit_posts["name"].unique())
    related_post_filter = raw_reddit_comments["link_id"].apply(
        lambda x: x in related_post_ids
    )
    # with open("../resources/filter_keywords.txt", "r") as file:
    #     filter_keywords = [line.strip() for line in file.readlines() if line.strip()]
    filter_keywords = ("BYU", "General Conference")
    keyword_filter = raw_reddit_comments["body"].apply(
        lambda x: any(keyword.lower() in x.lower() for keyword in filter_keywords)
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
