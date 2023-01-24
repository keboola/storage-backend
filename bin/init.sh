#!/bin/bash
set -Eeuo pipefail

git config --global user.email "git@github.com"
git config --global user.name "Github Actions"
git init
git checkout --orphan main || true
git commit -m "initial commit" --allow-empty
export REPO=php-datatypes
bin/adopt-repo.sh https://github.com/keboola/$REPO.git packages/$REPO $REPO/
#export REPO=php-table-backend-utils
#/monorepo-tools/adopt-repo.sh https://github.com/tomasfejfar/$REPO.git packages/$REPO $REPO/

echo "SPLITTING"

export REPO=php-datatypes
/monorepo-tools/split-repo.sh . http://example.com/$REPO packages/$REPO $REPO/

export REPO=php-table-backend-utils
/monorepo-tools/split-repo.sh . http://example.com/$REPO packages/$REPO $REPO/
