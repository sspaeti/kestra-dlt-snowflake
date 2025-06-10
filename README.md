# Kestra Snowflake dlt example


## Configure ENV variable
Specify `database`, `username`, `warehouse` and `role` in `dlt-chess-snowflake.py`. My example:
```python
    "database": "dlt_data",
    "username": "loader",
    "password": DESTINATION__SNOWFLAKE_PASSWORD,
    "warehouse": "COMPUTE_WH",
    "role": "DLT_LOADER_ROLE",
```

And create a `.env` and configure:
```
GITHUB_ACCESS_TOKEN=***
DESTINATION__SNOWFLAKE_CREDENTIALS=***
DESTINATION__SNOWFLAKE_PASSWORD=*** DESTINATION__SNOWFLAKE_HOST=***
```

After that run `./encrypt.sh` which will encrypt it and prefixes `SECRET_` and creates a the file specified in `docker-compose.yml` called `.env_encoded`.
