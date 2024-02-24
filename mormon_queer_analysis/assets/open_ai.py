import base64
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    SourceAsset,
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
from mormon_queer_analysis.resources.duckdb_io_manager import Database
from mormon_queer_analysis.resources.open_client import OpenAIClientResource


class ClusterConfig(Config):
    n_clusters: int = Field(default=32, description="Number of clusters")


@asset(
    partitions_def=monthly_partitions,
    metadata={"partition_expr": "TO_TIMESTAMP(date)"},
    deps=[
        SourceAsset(key=AssetKey("topical_reddit_posts")),
        SourceAsset(key=AssetKey("topical_reddit_comments")),
    ],
)
def open_ai_embeddings(
    context: AssetExecutionContext,
    database: Database,
    open_ai_client: OpenAIClientResource,
) -> pd.DataFrame:
    """
    Generates embeddings for Reddit posts and comments using OpenAI's text-embedding model.
    This asset combines monthly-partitioned posts and comments, filters out overly long texts,
    and applies the embedding model to the remaining content.
    """

    start_date = context.partition_key
    end_date = (
        datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(months=1)
    ).strftime("%Y-%m-%d")

    query = f"""
        (
            SELECT
                date,
                score,
                text,
                subreddit
            FROM
                REDDIT.topical_reddit_posts
            WHERE
                TO_TIMESTAMP(date) >= '{start_date} 00:00:00'
                AND
                TO_TIMESTAMP(date) < '{end_date} 00:00:00'
        )
        UNION ALL
        (
            SELECT
                date,
                score,
                text,
                subreddit
            FROM
                REDDIT.topical_reddit_comments
            WHERE
                TO_TIMESTAMP(date) >= '{start_date} 00:00:00'
                AND
                TO_TIMESTAMP(date) < '{end_date} 00:00:00'
        )
        """

    df = database.query(query)
    if df is None or df.empty:
        return

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
    context.add_output_metadata(
        metadata={
            "rows_dropped": MetadataValue.int(rows_dropped),
        }
    )

    # OpenAI docs say this may take a few minutes
    df["embedding"] = df.text.apply(
        lambda x: open_ai_client.get_model_embedding(x, model=embedding_model)
    )
    context.log.info(f"Hit OpenAI client")
    df["embedding"] = df["embedding"].apply(np.array)

    return df


@asset(deps=[open_ai_embeddings])
def k_means_clustering(
    context: AssetExecutionContext,
    config: ClusterConfig,
    database: Database,
) -> pd.DataFrame:
    """
    Performs K-means clustering on OpenAI embeddings.

    Aggregates embeddings data from multiple partitions,
    converts them into a matrix, and applies K-means clustering algorithm based on the specified number
    of clusters in the configuration. Each row in the resultant DataFrame is labeled with its corresponding
    cluster. Marks observations that are closest to the cluster center to serve as sample texts. Makes a plot
    of the t-SNE clusters.
    """

    df = database.query(
        f"""
        SELECT
            *
        FROM
            REDDIT.open_ai_embeddings
    """
    )

    # Format embeddings
    df["embedding"] = df["embedding"].apply(np.array)
    matrix = np.vstack(df.embedding.values)

    # Perform k-means clustering
    n_clusters = config.n_clusters
    kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42)
    kmeans.fit(matrix)
    labels = kmeans.labels_
    df["cluster"] = labels

    # Mark observations that are closest to the cluster center to serve as sample texts
    n_samples = 10
    df["is_central_member"] = False

    for cluster_id in range(n_clusters):
        cluster_df = df[df["cluster"] == cluster_id]
        distances_to_center = np.linalg.norm(
            matrix[cluster_df.index] - kmeans.cluster_centers_[cluster_id], axis=1
        )
        cluster_df = cluster_df.copy()
        cluster_df["distance_to_center"] = distances_to_center
        # Sort the DataFrame based on the distance
        sorted_cluster_df = cluster_df.sort_values("distance_to_center")
        # Select the top N texts or all texts if there are fewer than N
        closest_texts = sorted_cluster_df.head(min(n_samples, len(sorted_cluster_df)))
        df.loc[closest_texts.index, "is_central_member"] = True

    # Plot the t-SNE clusters
    tsne = TSNE(
        n_components=2, perplexity=15, random_state=42, init="random", learning_rate=200
    )
    vis_dims2 = tsne.fit_transform(matrix)
    x, y = zip(*vis_dims2)
    colors = plt.cm.rainbow(np.linspace(0, 1, n_clusters))  # Create a colormap
    plt.figure(figsize=(15, 6))
    for category, color in enumerate(colors):
        xs = np.array(x)[df.cluster == category]
        ys = np.array(y)[df.cluster == category]
        plt.scatter(xs, ys, color=color, alpha=0.3)
        avg_x = xs.mean()
        avg_y = ys.mean()
        plt.scatter(avg_x, avg_y, marker="x", color=color, s=100)
    plt.title("Clusters identified visualized in language 2d using t-SNE")
    # Convert the image to Markdown to preview it within Dagster
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"
    context.add_output_metadata({"plot": MetadataValue.md(md_content)})

    return df


@asset
def cluster_summaries(
    context: AssetExecutionContext,
    config: ClusterConfig,
    k_means_clustering: pd.DataFrame,
    open_ai_client: OpenAIClientResource,
) -> pd.DataFrame:
    """
    OpenAI generated summaries for each cluster, and samples texts from each cluster.
    """
    df = k_means_clustering
    n_clusters = config.n_clusters
    cluster_info = []
    model = "gpt-3.5-turbo-1106"

    summary_prompt = "What do the following reddit posts and comments have in common, beyond being related to Mormonism and LGBTQ+ issues? On theme, argumentation style, tone, etc.?\n\nReddit posts and comments:\n"
    title_prompt = "Provide a concise title for the following Reddit posts and comments related to Mormonism and LGBTQ+ issues.\n\nReddit posts and comments:\n"

    for i in range(n_clusters):
        sample_texts_df = df[(df.cluster == i) & (df.is_central_member)]
        texts = "\n".join(sample_texts_df.text.values)

        # Prepare the summary prompt for OpenAI and get the summary
        summary_messages = [{"role": "system", "content": summary_prompt + texts}]
        context.log.info(f"API Hit for Summary # {i}")
        summary_response = open_ai_client.completions_with_backoff(
            model, summary_messages
        )
        summary_content = summary_response.choices[0].message.content

        # Prepare the title prompt for OpenAI and get the title
        title_messages = [{"role": "system", "content": title_prompt + texts}]
        context.log.info(f"API Hit for Title # {i}")
        title_response = open_ai_client.completions_with_backoff(model, title_messages)
        title_content = title_response.choices[0].message.content

        # Append the summary, title, and cluster ID to our list
        cluster_info.append(
            {"cluster": i, "summary": summary_content, "title": title_content}
        )

    return pd.DataFrame(cluster_info)
