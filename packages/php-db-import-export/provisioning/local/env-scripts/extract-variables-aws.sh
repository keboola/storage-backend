#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh $TF_OUTPUTS_FILE

# output variables
output_var 'AWS_REGION' $(terraform_output 'S3FileStorageRegion')
output_var 'AWS_ACCESS_KEY_ID' $(terraform_output 'S3FileStorageAwsKey')
output_var 'AWS_SECRET_ACCESS_KEY' $(terraform_output 'S3FileStorageAwsSecret')
output_var 'AWS_S3_BUCKET' $(terraform_output 'FileStorageBucket')
output_var 'AWS_S3_KEY' 'exp-2'

echo ""
