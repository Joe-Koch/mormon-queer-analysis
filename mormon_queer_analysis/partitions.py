from dagster import (
    EnvVar,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

SUBREDDITS = ["lds", "latterdaysaints", "mormon", "exmormon"]
START_DATE = EnvVar("START_DATE").get_value()
END_DATE = EnvVar("END_DATE").get_value()


def reddit_partitions_def(start_date, end_date):
    return MultiPartitionsDefinition(
        {
            "date": MonthlyPartitionsDefinition(
                start_date=start_date,
                end_date=end_date,
            ),
            "subreddit": StaticPartitionsDefinition(SUBREDDITS),
        }
    )


def monthly_partitions_def(start_date, end_date):
    return MonthlyPartitionsDefinition(
        start_date=start_date,
        end_date=end_date,
    )


reddit_partitions = reddit_partitions_def(START_DATE, END_DATE)
monthly_partitions = monthly_partitions_def(START_DATE, END_DATE)
