#!/usr/bin/env bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)


startDb(){
    curl -X PUT "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases/$EXA_SAAS_DB_ID/start" -H "accept: application/json" -H "Authorization: Bearer $EXA_SAAS_TOKEN" -s
}
stopDb(){
    curl -X PUT "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases/$EXA_SAAS_DB_ID/stop" -H "accept: application/json" -H "Authorization: Bearer $EXA_SAAS_TOKEN" -s
}
getDb(){
    curl -X GET "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases/$EXA_SAAS_DB_ID" -H "accept: application/json" -H "Authorization: Bearer $EXA_SAAS_TOKEN" -s
}

getDbStatus(){
  echo $(getDb | jq -r ".status")
}


# DESC: Calls getStatus in loop to test if server is online
waitForStart(){
    local runtime="10 minute"
    local endtime=$(date -ud "$runtime" +%s)

    while [[ $(date -u +%s) -le ${endtime} ]]
    do
        local status=$(getDbStatus)
        echo "Exasol server status: "${status}
        case $status in
            "running")
              echo "Exasol server is online."
              exit 0
                ;;
            "starting")
                echo "Exasol is resuming waiting 10s"
                sleep 10
                ;;
            "tostart")
                echo "Exasol is queued to start waiting 10s"
                sleep 10
                ;;
            "stopped")
                echo "Exasol is paused running resume"
                startDb &
                sleep 10
                ;;
            "stopping")
                echo "Exasol is pausing waiting 10s"
                sleep 10
                ;;
            "tostop")
                echo "Exasol is queued to stop waiting 10s"
                sleep 10
                ;;
            *)
              echo "Unknown error: Exasol server status is: "${status}
              exit 1
                ;;
        esac
    done

    echo "Exasol DB is not running after 10 minutes." >> /dev/stderr
    exit 1
}

# DESC: Usage help
function script_usage() {
    cat << EOF
synapse.sh [--resume|--pause|--waitForStart]

Script for starting Exasol SaaS DB.

 Options:
  -h|--help                Print this
  -r|--resume              Resume DB
  -p|--pause               Pause DB
  -w|--waitForStart        Wait till DB is online, this will check api periodically
                           return 0 if server is online
                           return 1 if server is not online after 10 minutes.
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
            startDb
            ;;
        -p | --pause)
            stopDb
            ;;
        -w | --waitForStart)
            waitForStart
            ;;
        *)
            script_exit "Invalid parameter was provided: $param" 1
            ;;
    esac
done
