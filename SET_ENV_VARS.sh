# SET_ENV_VARS.sh
# Copy this file and edit the values, then run:
#   source ./SET_ENV_VARS.sh
# to export the environment variables into your current shell session.
#
# WARNING: This file will contain secrets if you put keys/passwords here. Do not commit
# it to version control. Add to .gitignore if needed.

# -------------------- Firebolt credentials --------------------
# Your Firebolt account name (e.g. 'zszec')

export FIREBOLT_ACCOUNT="account-1"
# Firebolt client ID for service account
export FIREBOLT_CLIENT_ID="<firebolt service account client id>"
# Firebolt client secret for service account
export FIREBOLT_CLIENT_SECRET="<firebolt server account secret>"
# Firebolt database and engine names
export FIREBOLT_DATABASE="sailing"
export FIREBOLT_ENGINE="my_engine"

# -------------------- AWS credentials (uploader) --------------------
# AWS access key id and secret for the uploader (long-lived or temporary)
export AWS_ACCESS_KEY_ID="<AWS S3 Bucket Access Key>"
export AWS_SECRET_ACCESS_KEY="<<WS S3 Bucket Access Key Secret>"
# If you are using temporary credentials (STS AssumeRole), also set this
# export AWS_SESSION_TOKEN="<AWS_SESSION_TOKEN>"
# Default AWS region used for S3 operations
export AWS_REGION="us-east-1"

# -------------------- S3 target info --------------------
# Bucket and prefix where staged CSVs will be uploaded
export S3_BUCKET="<S3 bucket>"
export S3_PREFIX="<S3 prefix>"

# Optional: Firebolt role/external-id if you will configure a role for Firebolt to assume
# export FIREBOLT_AWS_ACCOUNT_ID="<FIREBOLT_AWS_ACCOUNT_ID>"
# export FIREBOLT_EXTERNAL_ID="<FIREBOLT_EXTERNAL_ID>"

# Helpful: show a reminder after sourcing
echo "Environment variables loaded. (Remember: do not commit this file.)"
