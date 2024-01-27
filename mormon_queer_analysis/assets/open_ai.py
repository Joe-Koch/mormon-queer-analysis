import base64
from io import BytesIO
from typing import Dict

from dagster import (
    AssetExecutionContext,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pydantic import Field
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
import tiktoken

from mormon_queer_analysis.partitions import monthly_partitions
from mormon_queer_analysis.resources.open_client import OpenAIClientResource
from mormon_queer_analysis.utils.embeddings_utils import get_embedding


class ClusterConfig(Config):
    n_clusters: int = Field(default=60, description="Number of clusters")


@asset(partitions_def=monthly_partitions)
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
    openai = open_ai_client.get_client()
    df["embedding"] = df.text.apply(
        lambda x: get_embedding(openai, x, model=embedding_model)
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
    open_ai_client: OpenAIClientResource,
) -> MaterializeResult:
    """
    Visualizes the clustering results obtained from K-means clustering of OpenAI embeddings.
    This asset performs two main visualizations: a t-SNE plot to represent clusters in a
    2D space and a time series plot showing the frequency of observations per cluster over time.

    Also uses OpenAI to generate summaries for each cluster, and samples and displays texts from each
    cluster.
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

    x = [x for x, y in vis_dims2]
    y = [y for x, y in vis_dims2]

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
    plt.subplot(1, 2, 2)
    for category, color in enumerate(colors):
        cluster_data = df[df.cluster == category]
        # Convert 'date' column to datetime
        cluster_data["date"] = pd.to_datetime(cluster_data["date"])
        # Set 'date' as the index
        cluster_data.set_index("date", inplace=True)
        # Resample and count observations per month
        monthly_data = cluster_data.resample("M").size()
        monthly_data.plot(
            kind="line", color=color, alpha=0.7, label=f"Cluster {category}"
        )

    plt.legend(title="Cluster", bbox_to_anchor=(1.05, 1), loc="upper left")

    # Adjusting the y-axis limits if needed
    plt.ylim(
        ymin=0
    )  # Set the bottom of the y-axis to 0, or choose an appropriate lower bound

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

    # CLUSTER SUMMARIES AND SAMPLES
    # Reading a text sample which belongs to each group.
    sample_per_cluster = 10

    summaries = ""
    sample_texts = ""

    for i in range(n_clusters):
        cluster_df = df[df.cluster == i]
        n_samples = min(10, cluster_df.shape[0])
        sample_cluster_rows = cluster_df.sample(n_samples, random_state=42)

        # Make openAI summaries of each cluster
        summaries += f"Cluster {i} Theme: "

        texts = "\n".join(sample_cluster_rows.text.values)

        # TODO: switch model to gpt-4
        # Get a summary of each cluster from openAI
        client = open_ai_client.get_client()
        response = client.completions.create(
            model="gpt-3-turbo-instruct",
            role="system",
            prompt=f'What do the following reddit posts and comments have in common, beyond being related to Mormonism and LGBTQ+? On theme, argumentation style, tone, etc.?\n\nReddit posts and comments:\n"""\n{texts}\n"""',
            temperature=0,
            max_tokens=100,
            frequency_penalty=0,
        )

        summaries += response.choices[0].text.replace("\n", "") + "\n"

        # Display some sample texts from each cluster
        sample_texts += f"Cluster {i} Texts: "

        for j in range(n_samples):
            sample_texts += f"{sample_cluster_rows.score.values[j]}, "
            sample_texts += f"{sample_cluster_rows.date.values[j]}:   "
            sample_texts += f"{sample_cluster_rows.text.str[:300].values[j]}\n"

    # Attach the Markdown content as metadata to the asset
    return MaterializeResult(
        metadata={
            "plot": MetadataValue.md(md_content),
            "summaries": MetadataValue.md(summaries),
            "samples": MetadataValue.md(sample_texts),
        }
    )
