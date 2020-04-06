#!/usr/bin/env bash

set -o pipefail         # Use last non-zero exit code in a pipeline

#REQUIRED ENV VARS
#SYNAPSE_SERVER_NAME=
#AZURE_RESOURCE_GROUP=
#AZURE_SERVICE_PRINCIPAL_TENANT=
#AZURE_SERVICE_PRINCIPAL=
#AZURE_SERVICE_PRINCIPAL_PASSWORD=

#EXPORTED ENV VARS
#SYNAPSE_SERVER_PASSWORD=
#SYNAPSE_SQL_SERVER_NAME=
#SYNAPSE_DW_SERVER_NAME=
#SYNAPSE_RESOURCE_ID=
#SYNAPSE_UID=
#SYNAPSE_PWD=
#SYNAPSE_DATABASE=
#SYNAPSE_SERVER=


# DESC: Runs az cli cmd under principal login
runCliCmd(){
    docker run --volume $(pwd):/keboola quay.io/keboola/azure-cli  \
        sh -c "az login --service-principal -u $AZURE_SERVICE_PRINCIPAL -p $AZURE_SERVICE_PRINCIPAL_PASSWORD --tenant $AZURE_SERVICE_PRINCIPAL_TENANT >> /dev/null && $1"
}

createServer(){
    export SYNAPSE_SERVER_PASSWORD=`openssl rand -base64 32`
    export DEPLOYMENT_NAME=${SYNAPSE_SERVER_NAME}"_"`openssl rand -hex 5`

    local output=$(runCliCmd "az group deployment create \
  --name ${DEPLOYMENT_NAME} \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --template-file /keboola/provisioning/synapse/synapse.json \
  --output json \
  --parameters \
    administratorLogin=keboola \
    administratorPassword=${SYNAPSE_SERVER_PASSWORD} \
    warehouseName=${SYNAPSE_SERVER_NAME} \
    warehouseCapacity=900")
    export SYNAPSE_SQL_SERVER_NAME=$(echo ${output} | jq -r '.properties.outputs.sqlServerName.value')
    export SYNAPSE_DW_SERVER_NAME=$(echo ${output} | jq -r '.properties.outputs.warehouseName.value')
    export SYNAPSE_RESOURCE_ID=$(echo ${output} | jq -r '.properties.outputs.warehouseResourceId.value')

    echo "Server deployed."
    echo $SYNAPSE_SQL_SERVER_NAME
    echo $SYNAPSE_DW_SERVER_NAME
    echo $SYNAPSE_RESOURCE_ID

    runCliCmd "az sql server firewall-rule create \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --server ${SYNAPSE_SQL_SERVER_NAME} \
  --name all \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 255.255.255.255"

    echo "Firewall rule set."

#    Set vars for php app
    export SYNAPSE_UID=keboola
    export SYNAPSE_PWD=${SYNAPSE_SERVER_PASSWORD}
    export SYNAPSE_DATABASE=${SYNAPSE_DW_SERVER_NAME}
    export SYNAPSE_SERVER=${SYNAPSE_SQL_SERVER_NAME}.database.windows.net
}

deleteServer(){
    local output=$(runCliCmd "az sql dw delete -y \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --name ${SYNAPSE_DW_SERVER_NAME} \
  --server ${SYNAPSE_SQL_SERVER_NAME}")

  echo "Synapse deleted."
  echo $output

      local output=$(runCliCmd "az sql server delete -y \
  --resource-group ${AZURE_RESOURCE_GROUP} \
  --name ${SYNAPSE_SQL_SERVER_NAME}")

  echo "Logical SQL server deleted."
  echo $output

# no right for principal to delete deploy
# https://github.com/keboola/php-table-backend-utils/pull/1#discussion_r403919848
#        local output=$(runCliCmd "az deployment delete \
#  --name ${DEPLOYMENT_NAME}")
#
#  echo "Deployment deleted."
#  echo $output
}


# DESC: Usage help
function script_usage() {
    cat << EOF
synapse.sh [-c| -d| -h]

Script for starting azure synapse.

 Options:
  -h|--help                Print this
  -d|--delete              Create server
  -c|--create              Delete server
EOF
}

while [[ $# -gt 0 ]]; do
    param="$1"
    shift
    case $param in
        -h | --help)
            script_usage
            ;;
        -c )
            createServer
            ;;
        -d )
            deleteServer
            ;;
        *)
            script_usage
            ;;
    esac
done
