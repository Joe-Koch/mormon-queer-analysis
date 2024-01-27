from unittest.mock import patch

from dagster import build_asset_context
import numpy as np
import pandas as pd

from mormon_queer_analysis.assets.open_ai import (
    ClusterConfig,
    k_means_clustering,
    open_ai_embeddings,
)
from mormon_queer_analysis.assets.reddit import (
    raw_reddit_posts,
    topical_reddit_posts,
)


def test_raw_reddit_posts():
    mock_data = {
        "data": [
            {
                "created_utc": "2023-01-01",
                "score": 10,
                "name": "post1",
                "title": "Test Title",
                "selftext": "Test Content",
            }
        ]
    }
    context = build_asset_context(
        partition_key=str({"date": "2021-01-01", "subreddit": "main"})
    )
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_data
        result = raw_reddit_posts(context)
        assert len(result) == 1
        assert result.iloc[0]["title"] == "Test Title"


def test_topical_reddit_posts():
    mock_posts = pd.DataFrame(
        {
            "created_utc": ["2023-01-01"],
            "score": [10],
            "name": ["post1"],
            "title": ["Test Title Relevant Keyword"],
            "selftext": ["Test Content"],
        }
    )
    context = build_asset_context(
        partition_key=str({"date": "2021-01-01", "subreddit": "main"})
    )
    result = topical_reddit_posts(context, mock_posts)
    assert len(result) == 1
    assert result.iloc[0]["text"] == "Test Title Relevant Keyword\nTest Content"


def test_open_ai_embeddings():
    # Create mock data for input DataFrames
    mock_posts = pd.DataFrame({"text": ["sample post"]})
    mock_comments = pd.DataFrame({"text": ["sample comment"]})

    # Mock OpenAIClientResource and its methods
    with patch(
        "mormon_queer_analysis.assets.open_ai.OpenAIClientResource"
    ) as mock_client:
        mock_client.get_client.return_value.completions.create.return_value = {
            "choices": [{"text": "mock_embedding"}]
        }
        context = build_asset_context(
            partition_key=str({"date": "2021-01-01", "subreddit": "main"})
        )
        # Execute the asset with mock data and client
        result = open_ai_embeddings(
            context,
            topical_reddit_posts=mock_posts,
            topical_reddit_comments=mock_comments,
            open_ai_client=mock_client,
        )

        # Validate results
        assert not result.empty
        assert "embedding" in result.columns
        assert result.loc[0, "embedding"] == "mock_embedding"


def test_k_means_clustering():
    # Create mock data for embeddings
    mock_embeddings = {
        "partition1": pd.DataFrame({"embedding": [np.array([1, 2]), np.array([3, 4])]})
    }

    config = ClusterConfig(n_clusters=2)

    context = build_asset_context(
        partition_key=str({"date": "2021-01-01", "subreddit": "main"})
    )

    # Execute the asset with mock data
    result = k_means_clustering(
        context, config=config, open_ai_embeddings=mock_embeddings
    )

    # Validate results
    assert not result.empty
    assert "cluster" in result.columns
    assert result["cluster"].nunique() == config.n_clusters
