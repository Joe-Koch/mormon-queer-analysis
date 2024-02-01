import base64
from io import BytesIO
from typing import Dict

import backoff
from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
import matplotlib.pyplot as plt
import numpy as np
from openai import RateLimitError
import pandas as pd
from pydantic import Field
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
import tiktoken

from mormon_queer_analysis.partitions import reddit_partitions
from mormon_queer_analysis.resources.open_client import OpenAIClientResource
from mormon_queer_analysis.utils.embeddings_utils import get_embedding


class ClusterConfig(Config):
    n_clusters: int = Field(default=60, description="Number of clusters")


@asset(partitions_def=reddit_partitions)
def open_ai_embeddings(
    context: AssetExecutionContext,
    topical_reddit_posts: pd.DataFrame,
    topical_reddit_comments: pd.DataFrame,
    open_ai_client: OpenAIClientResource,
) -> pd.DataFrame:
    """
    Generates embeddings for Reddit posts and comments using OpenAI's text-embedding model.
    This asset combines monthly-partitioned posts and comments, filters out overly long texts,
    and applies the embedding model to the remaining content.
    """

    df = pd.DataFrame()
    # Combine reddit posts and comments, if they exist (some partitions didn't capture any topical content)
    if topical_reddit_posts.any:
        df = pd.concat([df, topical_reddit_posts], ignore_index=True)
    if topical_reddit_comments.any:
        df = pd.concat([df, topical_reddit_comments], ignore_index=True)
    if df.empty:
        return df

    # embedding model parameters
    embedding_model = "text-embedding-ada-002"
    embedding_encoding = "cl100k_base"  # this the encoding for text-embedding-ada-002
    max_tokens = 8000  # the maximum for text-embedding-ada-002 is 8191
    encoding = tiktoken.get_encoding(embedding_encoding)

    # Omit input texts that are too long to embed
    df["n_tokens"] = df.text.apply(lambda x: len(encoding.encode(x)))
    initial_row_count = len(df)
    df = df[df.n_tokens <= max_tokens]
    final_row_count = len(df)
    rows_dropped = initial_row_count - final_row_count

    # OpenAI docs say this may take a few minutes
    client = open_ai_client.get_client()
    df["embedding"] = df.text.apply(
        lambda x: get_embedding(client, x, model=embedding_model)
    )

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
            "rows_dropped": MetadataValue.int(rows_dropped),
        }
    )

    return df


@asset
def k_means_clustering(
    context: AssetExecutionContext, config: ClusterConfig, open_ai_embeddings: Dict
) -> pd.DataFrame:
    """
    Performs K-means clustering on OpenAI embeddings.

    Aggregates embeddings data from multiple partitions,
    converts them into a matrix, and applies K-means clustering algorithm based on the specified number
    of clusters in the configuration. Each row in the resultant DataFrame is labeled with its corresponding
    cluster.
    """
    df = pd.DataFrame()
    # Iterate over each partition's data and concatenate it to the aggregated DataFrame
    for partition_df in open_ai_embeddings.values():
        df = pd.concat([df, partition_df], ignore_index=True)

    df["embedding"] = df["embedding"].apply(np.array)

    matrix = np.vstack(df.embedding.values)

    n_clusters = config.n_clusters

    kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42)
    kmeans.fit(matrix)
    labels = kmeans.labels_
    df["cluster"] = labels

    df.to_json("data/k_means_clustering.json")

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df


@asset
def cluster_visualization(
    config: ClusterConfig,
    k_means_clustering: pd.DataFrame,
) -> MaterializeResult:
    """
    Visualizes the clustering results obtained from K-means clustering of OpenAI embeddings.
    This asset performs two main visualizations: a t-SNE plot to represent clusters in a
    2D space and a time series plot showing the frequency of observations per cluster over time.
    """

    # CLUSTER VISUALIZATION
    df = k_means_clustering

    matrix = np.vstack(df.embedding.values)
    n_clusters = config.n_clusters

    df.groupby("cluster").score.mean().sort_values()

    tsne = TSNE(
        n_components=2, perplexity=15, random_state=42, init="random", learning_rate=200
    )
    vis_dims2 = tsne.fit_transform(matrix)
    x, y = zip(*vis_dims2)

    # Create a colormap
    colors = plt.cm.rainbow(np.linspace(0, 1, n_clusters))

    plt.figure(figsize=(15, 6))

    # Plotting the t-SNE clusters
    plt.subplot(1, 2, 1)
    for category, color in enumerate(colors):
        xs = np.array(x)[df.cluster == category]
        ys = np.array(y)[df.cluster == category]
        plt.scatter(xs, ys, color=color, alpha=0.3)
        avg_x = xs.mean()
        avg_y = ys.mean()
        plt.scatter(avg_x, avg_y, marker="x", color=color, s=100)

    plt.title("Clusters identified visualized in language 2d using t-SNE")

    # Plotting the time series

    selected_clusters = {1: "Deleted Posts", 2: "Angry Responses", 5: "Another Label"}

    plt.subplot(1, 2, 2)
    for category, label in selected_clusters.items():
        cluster_data = df[df.cluster == category]
        # Convert 'date' column to datetime
        # Convert Reddit's Unix timestamp to datetime format.
        cluster_data["date"] = pd.to_datetime(cluster_data["date"], unit="s")
        # Set 'date' as the index
        cluster_data.set_index("date", inplace=True)
        # Resample and count observations per month
        yearly_data = cluster_data.resample("Y").size()
        yearly_data.plot(kind="line", alpha=0.7, label=label)

    plt.title("Frequency of Observations per Cluster Over Time")
    plt.xlabel("Date")
    plt.ylabel("Frequency")

    plt.tight_layout(
        rect=[0, 0, 0.85, 1]
    )  # Adjust the rect to make space for the legend

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(
        metadata={
            "plot": MetadataValue.md(md_content),
        }
    )


@backoff.on_exception(backoff.expo, RateLimitError)
def completions_with_backoff(client, messages):
    """Use OpenAI's chat completions API, but back off if it gets a rate limit error"""
    response = client.chat.completions.create(
        model="gpt-3.5-turbo-1106",
        messages=messages,
    )
    return response


@asset
def cluster_summaries(
    config: ClusterConfig,
    k_means_clustering: pd.DataFrame,
    open_ai_client: OpenAIClientResource,
) -> MaterializeResult:
    """
    OpenAI generated summaries for each cluster, and samples texts from each cluster.
    """

    # CLUSTER SUMMARIES AND SAMPLES
    df = k_means_clustering
    n_clusters = config.n_clusters
    sample_per_cluster = 8

    summaries = ""
    cluster_texts = ""
    client = open_ai_client.get_client()
    prompt = "What do the following reddit posts and comments have in common, beyond being related to Mormonism and LGBTQ+? On theme, argumentation style, tone, etc.?\n\nReddit posts and comments:\n"

    for i in range(n_clusters):
        cluster_df = df[df.cluster == i]
        n_samples = min(sample_per_cluster, cluster_df.shape[0])
        sample_cluster_rows = cluster_df.sample(n_samples, random_state=42)
        texts = "\n".join(sample_cluster_rows.text.values)

        # Add the texts for previewing a sample from each cluster
        cluster_texts += f"\n\nCluster {i} Samples: \n{texts}\n\n"

        # Get a summary of each cluster from openAI
        messages = [{"role": "system", "content": prompt + texts}]
        response = completions_with_backoff(client, messages)
        content = response.choices[0].message.content
        summaries += f"Cluster {i} Summary: \n{content}\n\n"

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(
        metadata={
            "summaries": MetadataValue.md(summaries),
            "samples": MetadataValue.md(cluster_texts),
        }
    )
