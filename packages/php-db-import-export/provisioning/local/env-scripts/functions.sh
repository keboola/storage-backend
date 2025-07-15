#!/usr/bin/env bash
set -Eeuo pipefail

TF_OUTPUTS_FILE=$1
SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_PATH}/../../.."

terraform_output() {
  jq ".${1}.value" -r $TF_OUTPUTS_FILE
}

terraform_output_json() {
  jq ".${1}.value" -r $TF_OUTPUTS_FILE | jq -c
}

output_var() {
  echo "${1}=\"${2}\""
}

output_var_no_ticks() {
  echo "${1}=${2}"
}

output_var_json() {
  echo "${1}='${2}'"
}

output_file() {
  mkdir -p "${PROJECT_ROOT}/$(dirname "${1}")"
  echo "${2}" >"${PROJECT_ROOT}/${1}"
}
