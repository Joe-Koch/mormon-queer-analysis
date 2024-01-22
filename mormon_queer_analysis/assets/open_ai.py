import base64
from io import BytesIO
from typing import Dict

from dagster import (
    AssetExecutionContext,
    # EventMetadataEntry,
    MetadataValue,
    # MaterializeResult,
    asset,
)
import matplotlib.pyplot as plt
import numpy as np
import openai
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE
import tiktoken

from mormon_queer_analysis.partitions import monthly_partitions
from mormon_queer_analysis.utils.embeddings_utils import get_embedding


@asset(partitions_def=monthly_partitions)
def open_ai_embeddings(
    context: AssetExecutionContext,
    topical_reddit_posts: pd.DataFrame,
    topical_reddit_comments: pd.DataFrame,
) -> pd.DataFrame:
    df = pd.concat([topical_reddit_posts, topical_reddit_comments], ignore_index=True)

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
    df["embedding"] = df.text.apply(lambda x: get_embedding(x, model=embedding_model))

    context.add_output_metadata(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(df.head().to_markdown()),
            # todo: Log rows_dropped as metadata
            # "rows_dropped": MetadataValue.int(
            #     rows_dropped, label="Number of rows dropped due to excess length"
            # ),
        }
    )

    return df


@asset
def k_means_clustering(open_ai_embeddings: Dict):
    df = pd.DataFrame()
    # Iterate over each partition's data and concatenate it to the aggregated DataFrame
    for partition_df in open_ai_embeddings.values():
        df = pd.concat([df, partition_df], ignore_index=True)

    df["embedding"] = df["embedding"].apply(np.array)

    matrix = np.vstack(df.embedding.values)
    matrix.shape

    n_clusters = 4

    kmeans = KMeans(n_clusters=n_clusters, init="k-means++", random_state=42)
    kmeans.fit(matrix)
    labels = kmeans.labels_
    df["cluster"] = labels

    df.groupby("cluster").score.mean().sort_values()

    tsne = TSNE(
        n_components=2, perplexity=15, random_state=42, init="random", learning_rate=200
    )
    vis_dims2 = tsne.fit_transform(matrix)

    x = [x for x, y in vis_dims2]
    y = [y for x, y in vis_dims2]

    for category, color in enumerate(["purple", "green", "red", "blue"]):
        xs = np.array(x)[df.cluster == category]
        ys = np.array(y)[df.cluster == category]
        plt.scatter(xs, ys, color=color, alpha=0.3)

        avg_x = xs.mean()
        avg_y = ys.mean()

        plt.scatter(avg_x, avg_y, marker="x", color=color, s=100)
    plt.title("Clusters identified visualized in language 2d using t-SNE")

    # Convert the image to a saveable format
    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    image_data = base64.b64encode(buffer.getvalue())

    # Convert the image to Markdown to preview it within Dagster
    md_content = f"![img](data:image/png;base64,{image_data.decode()})"

    # Attach the Markdown content as metadata to the asset
    # return MaterializeResult(metadata={"plot": MetadataValue.md(md_content)})
    return df


@asset
def cluster_summaries(context: AssetExecutionContext, k_means_clustering: pd.DataFrame):
    df = k_means_clustering
    n_clusters = 4

    # Reading a text sample which belongs to each group.
    sample_per_cluster = 5

    summaries = ""

    for i in range(n_clusters):
        summaries += f"Cluster {i} Theme: "

        texts = "\n".join(
            df[df.cluster == i]
            .text.str.replace("Title: ", "")
            .str.replace("\n\nContent: ", ":  ")
            .sample(sample_per_cluster, random_state=42)
            .values
        )
        # Get a summary of each cluster from openAI
        response = openai.completions.create(
            model="gpt-3.5-turbo-instruct",
            prompt=f'What do the following reddit posts and comments have in common?\n\nReddit posts and comments:\n"""\n{texts}\n"""\n\nTheme:',
            temperature=0,
            max_tokens=64,
            frequency_penalty=0,
        )

        summaries += response.choices[0].text.replace("\n", "") + "\n"
        context.log.info(f"Response is {response}")

        sample_cluster_rows = df[df.cluster == i].sample(
            sample_per_cluster, random_state=42
        )
        for j in range(sample_per_cluster):
            summaries += f"{sample_cluster_rows.score.values[j]}, "
            summaries += f"{sample_cluster_rows.date.values[j]}:   "
            summaries += f"{sample_cluster_rows.text.str[:70].values[j]}\n"

        summaries += "-" * 100 + "\n"

        return summaries
