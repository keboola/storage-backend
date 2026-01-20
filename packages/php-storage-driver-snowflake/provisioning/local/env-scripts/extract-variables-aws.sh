#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1

cd "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source ./functions.sh $TF_OUTPUTS_FILE

# output variables
output_var 'AWS_S3_BUCKET' $(terraform_output 'FileStorageBucket')
output_var 'AWS_S3_KEY' 'exp-2'

echo ""
