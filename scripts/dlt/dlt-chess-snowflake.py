import dlt
from chess import source
from kestra import Kestra
import os


print("Script starting")
DESTINATION__SNOWFLAKE_PASSWORD = os.getenv("DESTINATION__SNOWFLAKE_PASSWORD")
DESTINATION__SNOWFLAKE_HOST = os.getenv("DESTINATION__SNOWFLAKE_HOST")
print(f"##### DESTINATION__SNOWFLAKE_HOST: {DESTINATION__SNOWFLAKE_HOST}")


print("Credentials start")
credentials = {
    "host": DESTINATION__SNOWFLAKE_HOST,
    "database": "dlt_data",
    "username": "loader",
    "password": DESTINATION__SNOWFLAKE_PASSWORD,
    "warehouse": "COMPUTE_WH",
    "role": "DLT_LOADER_ROLE",
}
print(f"credentials: {credentials}")

snow_ = dlt.destinations.snowflake(credentials=credentials)

def load_players_games_example(start_month: str, end_month: str) -> None:
    """Constructs a pipeline that will load chess games of specific players for a range of months."""

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination=snow_,
        dataset_name="chess_players_games_data",
    )
    # create the data source by providing a list of players and start/end month in YYYY/MM format
    data = source(
        ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        start_month=start_month,
        end_month=end_month,
    )
    # load the "players_games" and "players_profiles" out of all the possible resources
    info = pipeline.run(data.with_resources("players_games", "players_profiles"))
    print(f"load_players_games_example: {info}")

def load_players_online_status() -> None:
    """Constructs a pipeline that will append online status of selected players"""

    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination=snow_,
        dataset_name="chess_players_games_data",
    )
    data = source(["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"])
    info = pipeline.run(data.with_resources("players_online_status"))
    print(f"load_players_online_status: {info}")

def load_players_games_incrementally() -> None:
    """Pipeline will not load the same game archive twice"""
    load_players_games_example("2024/11", "2025/02")


if __name__ == "__main__":
    # RUN PIPELINE HERE
    load_players_games_incrementally()
    load_players_online_status()
