from dagster import MonthlyPartitionsDefinition

monthly_partitions = MonthlyPartitionsDefinition(
    start_date="2005-06-01", end_date="2023-03-01"
)
