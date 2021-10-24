#!/usr/bin/env bash

set -o errexit          # Exit on most errors (see the manual)
set -o errtrace         # Make sure any error trap is inherited
set -o nounset          # Disallow expansion of unset variables
set -o pipefail         # Use last non-zero exit code in a pipeline
#set -o xtrace          # Trace the execution of the script (debug)


createDb(){
      DATE=`date +%s`
      curl -X 'POST' \
      "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases" \
      -H 'accept: application/json' \
      -H "Authorization: Bearer $EXA_SAAS_TOKEN" \
      -H 'Content-Type: application/json' \
      -d '{
      "name": "ci-ie-'$DATE'",
      "initialCluster": {
        "name": "ClusterName",
        "size": "XS"
      },
      "provider": "aws",
      "region": "eu-central-1"
    }'
}

getDb(){
    curl -X GET "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases/$1" -H "accept: application/json" -H "Authorization: Bearer $EXA_SAAS_TOKEN"
}
getClusters(){
    curl -X GET "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases/$1/clusters" -H "accept: application/json" -H "Authorization: Bearer $EXA_SAAS_TOKEN"
}
getConnectionData(){
    curl -X GET "$EXA_SAAS_HOST/api/v1/accounts/$EXA_SAAS_USER_ID/databases/$1/clusters/$2/connect" -H "accept: application/json" -H "Authorization: Bearer $EXA_SAAS_TOKEN"
}

getDbStatus(){
  echo $(getDb $1 | jq -r ".status")
}
createDbWithId(){
  echo $(createDb | jq -r ".id")
}
getClusterFromDb(){
  echo $(getClusters $1 | jq -r ".[0].id")
}
getDNS(){
  local connectionData=$(getConnectionData $1 $2)
  echo `echo $connectionData | jq -r ".dns"`:`echo $connectionData | jq -r ".port"`
}

#{
#  "status": "creating",
#  "id": "xxxx",
#  "name": "ci-import",
#  "clusters": {
#    "total": 1,
#    "running": 0
#  },
#  "provider": "aws",
#  "region": "eu-central-1",
#  "createdAt": "2021-10-18T11:01:20",
#  "createdBy": "saasengine"
#}



# DESC: Calls getStatus in loop to test if server is online
waitForCreate(){
    local runtime="30 minute"
    local endtime=$(date -ud "$runtime" +%s)
    local newDbId=$(createDbWithId)
    while [[ $(date -u +%s) -le ${endtime} ]]
    do
        local status=$(getDbStatus $newDbId)
        echo "Exasol DB status: "${status}
        if [[ ${status} == "running" ]]; then
            echo "Exasol DB is online."
            local clusterId=$(getClusterFromDb $newDbId)
            local dns=$(getDNS $newDbId $clusterId)
            echo $dns > out_dns
            exit 0
        fi
        # null -> tocreate -> creating -> running
        if [ "$status" != "tocreate" ] && [ "$status" != "creating" ]; then
            echo "Exasol DB is not ready yet Status: "${status}
            exit 1
        fi
        sleep 10
    done

    echo "Exasol DB is not running after 30 minutes."
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
        -c | --create)
            startDb
            ;;
        -d | --delete)
            deleteDb
            ;;
        -p | --pause)
            stopDb
            ;;
        -w | --waitForStart)
            waitForCreate
            ;;
        *)
            script_exit "Invalid parameter was provided: $param" 1
            ;;
    esac
done
