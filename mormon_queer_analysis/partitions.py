from dagster import (
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

SUBREDDITS = ["lds", "latterdaysaints", "mormon", "exmormon"]
START_DATE = "2005-06-01"
END_DATE = "2023-03-01"


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


reddit_partitions = reddit_partitions_def(START_DATE, END_DATE)
