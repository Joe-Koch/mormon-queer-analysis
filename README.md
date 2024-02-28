# mormon_queer_analysis

In this guide, we'll walk through a fully featured Dagster project that takes advantage of a wide range of Dagster features. This example can be useful as a point of reference for [using different Dagster APIs](#using-dagster-concepts) and [integrating other tools](#integrating-other-tools).

At a high level, this project consists of three asset groups, all centered around a contrived organization that wants to do ML and analysis on [Hacker News](https://news.ycombinator.com/) user activity data.



## Using Dagster concepts

This example shows useful patterns for many Dagster concepts, including:

### Organizing your assets in groups

[Software-defined assets](/concepts/assets/software-defined-assets) - An asset is a software object that models a data asset. The prototypical example is a table in a database or a file in cloud storage.

This project contains three asset groups:

<Image
alt="Global Asset Lineage"
src="/images/guides/fully-featured-project/global-asset-lineage.png"
width={4064}
height={2488}
/>

- [`Reddit`](https://github.com/Joe-Koch/mormon-queer-analysis/blob/main/mormon_queer_analysis/assets/reddit.py): Retrieves reddit posts and comments from LDS subreddits, and filters it down to media relevant to LGBTQ+ issues.

- [`OpenAI`](https://github.com/Joe-Koch/mormon-queer-analysis/blob/main/mormon_queer_analysis/assets/open_ai.py): Creates OpenAI embeddings for reddit posts and comments, performs K-means clustering, and generates cluster summaries using the ChatGPT API. 

- [`Visualization`](https://github.com/Joe-Koch/mormon-queer-analysis/blob/main/mormon_queer_analysis/assets/streamlit.py): Exports a database to be used in the final streamlit visualization.

### Varying external services or I/O without changing your DAG

[Resources](/concepts/resources) - A resource is an object that models a connection to a (typically) external service. Resources can be shared between assets, and different implementations of resources can be used depending on the environment. In this example, we built [multiple Hacker News API resources](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/hn_resource.py), all of which have the same interface but different implementations:

- `HNAPIClient` interacts with the real Hacker News API and gets the full data set, which will be used in production.
- `HNAPISubsampleClient` talks to the real API but subsamples the data, which is much faster than the normal implementation and is great for demoing purposes.
- `HNSnapshotClient` reads from a local snapshot, which is useful for unit testing or environments where the connection isn't available.

The way we model resources helps separate the business logic in code from environments, e.g. you can easily switch resources without changing your pipeline code.

[I/O managers](/concepts/io-management/io-managers) - An I/O manager is a special kind of resource that handles storing and loading assets. This example includes [a wide range of I/O managers](https://github.com/dagster-io/dagster/tree/master/examples/project_fully_featured/project_fully_featured/resources) such as:

- [`ParquetIOManager`](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py): stores outputs as parquet files in your local file system. It minimizes setup difficulty and is useful for local development.
- [DuckDBIOManager](/\_apidocs/libraries/dagster-duckdb#dagster_duckdb): Dagster provided DuckDB I/O manager that can store and load data as Pandas or PySpark DataFrames. Useful for local development since DuckDB runs locally and requires minimal setup. In this example, a DuckDB I/O manager that can handle Pandas and PySpark DataFrames is built using the <PyObject module="dagster_duckdb" object="build_duckdb_io_manager" /> function.
- [`SnowflakeIOManager`](/integrations/snowflake): Dagster provided Snowflake I/O manager that can store and load data as Pandas or PySpark DataFrames. In this example, a Snowflake I/O manager that can handle Pandas and PySpark DataFrames is built using the <PyObject module="dagster_snowflake" object="build_snowflake_io_manager" /> function.

### Scheduling and triggering jobs

[Schedules](/concepts/partitions-schedules-sensors/schedules) - A schedule allows you to execute a [job](/concepts/ops-jobs-graphs/jobs) at a fixed interval. This example includes an [hourly schedule](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/jobs.py) that materializes the `core` asset group every hour.

[Sensors](/concepts/partitions-schedules-sensors/sensors) - A sensor allows you to instigate runs based on some external state change. In this example, we have sensors to react to different state changes:

- [Send a Slack message on run failure](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/sensors/slack_on_failure_sensor.py)
- [Launch a given job when the materialized tables have been updated](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/sensors/hn_tables_updated_sensor.py)

### Testing

[Testing](/concepts/testing) - All Dagster entities are unit-testable. This example illustrates lightweight invocations in unit tests, including:

- [Testing sensors](https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured_tests/test_sensors) by mocking event storage and ticks and verifying definitions can load. Visit [Testing sensors](/concepts/partitions-schedules-sensors/sensors#testing-sensors) for more details.
- [Testing assets](https://github.com/dagster-io/dagster/tree/master/examples/project_fully_featured/project_fully_featured_tests/recommender_tests/assets_tests) by directly invoking the <PyObject object="asset" decorator />-decorated functions. Read more about testing assets on the [Testing](https://docs.dagster.io/concepts/testing#testing-software-defined-assets) page.
- [Testing I/O managers](https://github.com/dagster-io/dagster/tree/master/examples/project_fully_featured/project_fully_featured_tests/test_resources) by mocking the I/O and constructing <PyObject object="OutputContext" /> and <PyObject object="InputContext" /> with the mocks. Check out [Testing an IO manager](/concepts/io-management/io-managers#testing-an-io-manager) to learn more.

### Environments

This example is meant to be loaded from three deployments:

- A production deployment, which stores assets in S3 and Snowflake.
- A staging deployment, which stores assets in S3 and Snowflake, under a different key and database.
- A local deployment, which stores assets in the local filesystem and DuckDB.

By default, it will load for the local deployment. You can toggle deployments by setting the `DAGSTER_DEPLOYMENT` env var to `prod` or `staging`.

## Conclusion

As time goes on, this guide will be kept up to date, taking advantage of new Dagster features and learnings from the community. If you have anything you'd like to add, or an additional example you'd like to see, don't hesitate to reach out!








---

## Getting started

<CodeReferenceLink filePath="examples/project_fully_featured/" />

To follow along with this guide, you can bootstrap your own project with this example:

```bash
dagster project from-example \
    --name my-dagster-project \
    --example project_fully_featured
```

To install this example and its Python dependencies, run:

```bash
cd my-dagster-project
pip install -e .
```

Once you've done this, you can run:

```bash
dagster dev
```

to view this example in the Dagster UI.

---

## Getting started

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

Then, start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

You can start writing assets in `mormon_queer_analysis/assets.py`. The assets are automatically loaded into the Dagster code location as you define them.

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.
```bash
poetry install 
```

### Unit testing

Tests are in the `mormon_queer_analysis_tests` directory and you can run tests using `pytest`:

```bash
pytest mormon_queer_analysis_tests
```





# stuff

The stack:
- poetry
- dagster
- duckdb
- pandas 
- openAI 
- scikitlearn for k-means clustering
- 


K-means clustering 
why 32? 

