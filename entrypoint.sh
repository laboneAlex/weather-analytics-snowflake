#!/bin/bash

# If the SNOWFLAKE_PRIVATE_KEY secret is present, write it safely to temporary memory
if [ -n "$SNOWFLAKE_PRIVATE_KEY" ]; then
  echo "$SNOWFLAKE_PRIVATE_KEY" > /tmp/snowflake_key.p8
  chmod 600 /tmp/snowflake_key.p8
fi

# Hand execution back over to the original Airflow startup scripts
exec /entrypoint "$@"
