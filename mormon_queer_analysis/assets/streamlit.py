from dagster import AssetKey, SourceAsset, asset

from mormon_queer_analysis.resources.duckdb_io_manager import Database


@asset(
    deps=[
        SourceAsset(key=AssetKey("k_means_clustering")),
        SourceAsset(key=AssetKey("cluster_summaries")),
    ]
)
def streamlit_db(
    database: Database,
) -> None:
    """
    Copy the data used for streamlit visualizations into its own database
    """
    streamlit_db_path = "database/streamlit.duckdb"
    sql_query = f"""
        ATTACH '{streamlit_db_path}' AS streamlit_db;

        DROP TABLE IF EXISTS streamlit_db.reddit;
        CREATE TABLE streamlit_db.reddit AS 
        SELECT 
            date, 
            cluster, 
            text, 
            subreddit, 
            score, 
            is_central_member 
        FROM 
            reddit.k_means_clustering;

        DROP TABLE IF EXISTS streamlit_db.cluster_summaries;
        CREATE TABLE streamlit_db.cluster_summaries AS 
        SELECT 
            cluster, 
            title, 
            summary 
        FROM 
            reddit.cluster_summaries;
    """

    database.execute(sql_query)
    return
