#!/usr/bin/env bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)

#REQUIRED ENV VARS
#SYNAPSE_PRINCIPAL=http://xxx
#SYNAPSE_PRINCIPAL_PASSWORD=xxx
#SYNAPSE_PRINCIPAL_TENANT=xxx
#SYNAPSE_RESOURCE_GROUP=xxx
#SYNAPSE_SQL_SERVER_NAME=xxx
#SYNAPSE_DW_SERVER_NAME=xxx


# DESC: Runs az cli cmd under principal login
runCliCmd(){
    docker run quay.io/keboola/azure-cli \
        sh -c "az login --service-principal -u $SYNAPSE_PRINCIPAL -p $SYNAPSE_PRINCIPAL_PASSWORD --tenant $SYNAPSE_PRINCIPAL_TENANT >> /dev/null && az $1"
}

# DESC: Resume synapse
resume(){
    local output=$(runCliCmd "sql dw resume \
        --resource-group ${SYNAPSE_RESOURCE_GROUP} \
        --server ${SYNAPSE_SQL_SERVER_NAME} \
        --name ${SYNAPSE_DW_SERVER_NAME}")
    exit 0
}

# DESC: Pause synapse
pause(){
    local output=$(runCliCmd "sql dw pause \
        --resource-group ${SYNAPSE_RESOURCE_GROUP} \
        --server ${SYNAPSE_SQL_SERVER_NAME} \
        --name ${SYNAPSE_DW_SERVER_NAME}")
    exit 0
}

# DESC: Calls dw and parse output
getStatus(){
    local status=$(runCliCmd "sql dw list \
        --resource-group ${SYNAPSE_RESOURCE_GROUP} \
        --server ${SYNAPSE_SQL_SERVER_NAME} \
        --query \"[?name=='${SYNAPSE_DW_SERVER_NAME}'].status\" \
        --output tsv")
    echo ${status}
}

# DESC: Calls getStatus in loop to test if server is online
waitForStart(){
    local runtime="20 minute"
    local endtime=$(date -ud "$runtime" +%s)

    while [[ $(date -u +%s) -le ${endtime} ]]
    do
        local status=$(getStatus)
        echo "Synapse server status: "${status} >> /dev/stdout
        if [[ ${status} == "Online" ]]; then
            echo "Synapse server is online." >> /dev/stdout
            exit 0
        fi
        if [[ "$status" != "Resuming" ]]; then
            echo "Synapse server is not resuming. Status: "${status} >> /dev/stderr
            exit 1
        fi
        sleep 10
    done

    echo "Synapse server is not running after 20 minutes." >> /dev/stderr
    exit 1
}

# DESC: Usage help
function script_usage() {
    cat << EOF
synapse.sh [--resume|--pause|--waitForStart]

Script for starting azure synapse.

 Options:
  -h|--help                Print this
  -r|--resume              Resume Server
  -p|--pause               Pause Server
  -w|--waitForStart        Wait till Server is online, this will check api periodically
                           return 0 if server is online
                           return 1 if server is not online after 5 minutes.
EOF
}

while [[ $# -gt 0 ]]; do
    param="$1"
    shift
    case $param in
        -h | --help)
            script_usage
            exit 0
            ;;
        -r | --resume)
            resume
            ;;
        -p | --pause)
            pause
            ;;
        -w | --waitForStart)
            waitForStart

            ;;
        *)
            script_exit "Invalid parameter was provided: $param" 1
            ;;
    esac
done
