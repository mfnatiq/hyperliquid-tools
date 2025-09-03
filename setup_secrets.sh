#!/bin/bash

# hack for getting .streamlit/secrets.toml

# exit immediately if a command fails
set -e

# -p flag means won't fail if it already exists
mkdir -p .streamlit

# write the secrets.toml file by substituting environment variables.
# variable names must match prod env vars
echo "
[auth]
client_id = \"$AUTH_CLIENT_ID\"
client_secret = \"$AUTH_CLIENT_SECRET\"
redirect_uri = \"$AUTH_REDIRECT_URI\"
cookie_secret = \"$AUTH_COOKIE_SECRET\"
server_metadata_url = \"$AUTH_SERVER_METADATA_URL\"
" > .streamlit/secrets.toml

echo "✅ .streamlit/secrets.toml created successfully"

# executes the command that was passed to this script
# "streamlit run main.py ..."
exec "$@"