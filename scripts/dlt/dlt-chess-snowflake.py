import dlt
from chess import source
from kestra import Kestra
import os

# Function to decode base64-encoded environment variables
def get_decoded_env(env_name):
    encoded_value = os.environ.get(env_name)
    if encoded_value:
        try:
            # Decode the base64 value
            decoded_value = base64.b64decode(encoded_value).decode('utf-8')
            return decoded_value
        except Exception as e:
            print(f"Error decoding {env_name}: {e}")
            return None
    return None

print("Script starting")
DESTINATION__SNOWFLAKE_PASSWORD = os.getenv("SECRET_DESTINATION__SNOWFLAKE_PASSWORT")
DESTINATION__SNOWFLAKE_HOST = os.getenv("DESTINATION__SNOWFLAKE_HOST")
print("------------------")
print(f"##### DESTINATION__SNOWFLAKE_HOST: {DESTINATION__SNOWFLAKE_HOST}")

print(f"##### DESTINATION__SNOWFLAKE_HOST: {get_decoded_env(SECRET_DESTINATION__SNOWFLAKE_HOST}")

print("Credentials start")
credentials = {
    "host": DESTINATION__SNOWFLAKE_HOST,
    "database": "dlt_data",
    "username": "loader",
    "password": DESTINATION__SNOWFLAKE_PASSWORD,
    "warehouse": "COMPUTE_WH",
    "role": "DLT_LOADER_ROLE",
}

kestra.
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

    Kestra.outputs({'status' , info})
    # print(info)


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
    # print(info)
    Kestra.outputs({'status' , info})


def load_players_games_incrementally() -> None:
    """Pipeline will not load the same game archive twice"""
    # loads games for 11.2022
    load_players_games_example("2022/11", "2022/11")
    # second load skips games for 11.2022 but will load for 12.2022
    load_players_games_example("2022/11", "2022/12")


if __name__ == "__main__":
    # RUN PIPELINE HEREk
    # load_players_games_example("2022/11", "2023/05")
    load_players_games_incrementally()
    load_players_online_status()
