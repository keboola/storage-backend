#!/usr/bin/env bash
set -Eeuo pipefail

ENV_FILE=".env"
INSERT_MODE=prepend
VERBOSE=false

help () {
  echo "Syntax: update-env.sh [-v] [-a] [-e ${ENV_FILE}] <aws|azure|gcp>"
  echo "Options:"
  echo "  -a|--append           Append mode (used only when creating new env file, by default values are prepended to the env file)"
  echo "  -l|--localK8S         Extract envs from local k8s cluster localed in kubernetes folder"
  echo "  -e|--env-file file    Env file to write (default: ${ENV_FILE})"
  echo "  -v|--verbose          Output extra information"
  echo ""
  echo "Example: update-env.sh aws"
  echo "Example: update-env.sh -e .env.local azure"
  echo ""
}

LOCAL_K8S=false

POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -a|--append)
      INSERT_MODE=append
      shift
      ;;
    -e|--env-file)
      ENV_FILE="$2"
      shift
      shift
      ;;
    -v|--verbose)
      VERBOSE=true
      shift
      ;;
    -l|--localK8S)
      LOCAL_K8S=true
      shift
      ;;
    -h|--help)
      echo "Update env file with values from Terraform"
      echo ""
      help
      exit 0
      ;;
    -*|--*)
      echo "Unknown option $1"
      echo ""
      help
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done
set -- "${POSITIONAL_ARGS[@]}"

ENV_NAME=${1:-}
if [[ $ENV_NAME != "aws" && $ENV_NAME != "azure" && $ENV_NAME != "gcp" ]]; then
    echo "Invalid environment name '${ENV_NAME}'. Possible values are: aws, azure, gcp"
    echo ""
    help
    exit 1
fi

SCRIPT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_PATH}/../.."
cd "${PROJECT_ROOT}"

DELIMITER_START="##>> BEGIN GENERATED CONTENT <<##"
DELIMITER_END="##>> END GENERATED CONTENT <<##"

if [ ! -f "${ENV_FILE}" ]; then
  echo "Creating missing env file"
  touch "${ENV_FILE}"
fi

if [ "$LOCAL_K8S" = true ] ; then
  SCRIPT_PATH="${SCRIPT_PATH}/kubernetes"
  DELIMITER_START="##>> BEGIN K8S CONTENT <<##"
  DELIMITER_END="##>> END K8S CONTENT <<##"
  echo -e "Configuring \033[1;33m${ENV_FILE}\033[0m for local k8s cluster in folder \033[1;33m${SCRIPT_PATH}\033[0m"
else
  echo -e "Configuring \033[1;33m${ENV_FILE}\033[0m for \033[1;33m${ENV_NAME}\033[0m in folder \033[1;33m${SCRIPT_PATH}\033[0m"
fi

if ! grep -q "${DELIMITER_START}" "${ENV_FILE}"; then
  if [[ $INSERT_MODE == "append" ]]; then
      echo "Appending new auto-generated section to env file"
      echo "" >> "${ENV_FILE}"
      echo "${DELIMITER_START}" >> "${ENV_FILE}"
      echo "${DELIMITER_END}" >> "${ENV_FILE}"
  else
      echo "Prepending new auto-generated section to env file"
      ENV=$(cat "${ENV_FILE}")
      echo "${DELIMITER_START}" > "${ENV_FILE}"
      echo "${DELIMITER_END}" >> "${ENV_FILE}"
      echo "" >> "${ENV_FILE}"
      echo "${ENV}" >> "${ENV_FILE}"
  fi
fi

if [ "${VERBOSE}" = true ]; then
  echo "Terraform outputs"
  terraform -chdir="${SCRIPT_PATH}" output
  echo ""
fi

echo "Building variables"
ENV_TF_FILE="${ENV_FILE}.tf"
TF_OUTPUTS_FILE="${SCRIPT_PATH}/tfoutput.json"
trap "rm ${TF_OUTPUTS_FILE} || true; rm ${ENV_TF_FILE} || true" EXIT
terraform -chdir="${SCRIPT_PATH}" output -json > "${TF_OUTPUTS_FILE}"

if [ "$LOCAL_K8S" = true ] ; then
  "${SCRIPT_PATH}/extract-variables.sh" "$TF_OUTPUTS_FILE" "$PROJECT_ROOT" > "${ENV_TF_FILE}"
else
  "${SCRIPT_PATH}/env-scripts/extract-variables-common.sh" "$TF_OUTPUTS_FILE" > "${ENV_TF_FILE}"
  "${SCRIPT_PATH}/env-scripts/extract-variables-${ENV_NAME}.sh" "$TF_OUTPUTS_FILE" >> "${ENV_TF_FILE}"
fi

echo "Writing variables"

if [[ "$OSTYPE" == "darwin"* ]];
then
  sed -i '' -e "/${DELIMITER_START}/,/${DELIMITER_END}/{ /${DELIMITER_START}/{p; r ${ENV_TF_FILE}
        }; /${DELIMITER_END}/p; d; }" "${ENV_FILE}";
else
  sed -i'' -e "/${DELIMITER_START}/,/${DELIMITER_END}/{ /${DELIMITER_START}/{p; r ${ENV_TF_FILE}
        }; /${DELIMITER_END}/p; d; }" "${ENV_FILE}";
fi

echo "Done"

if [ "$LOCAL_K8S" = false ] ; then
  echo "Post update actions"
  "${SCRIPT_PATH}/env-scripts/info/generate-actions-${ENV_NAME}.sh" "$TF_OUTPUTS_FILE"
fi
