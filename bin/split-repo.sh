#!/usr/bin/env bash
set -e

if [[ -z ${1+x} || -z ${2+x} || -z ${3+x} || -z ${4+x} || -z ${5+x} ]]; then
  echo "Usage: split-repo.sh <source-repo-path> <target-repo-url> <library-path> <tag-prefix>"
  echo ""
  echo " <source-repo-path> Source Git repository path (the monorepo, may be also local path)"
  echo " <target-repo-url>  Target Git repository URL (the read-only library repo)"
  echo " <library-path>     Relative path to the library inside the source repo"
  echo " <tag-prefix>       Common prefix of tags to mirror. The prefix will be stripped from tags"
  echo " <last-tag>         Last tag, that has been present in singlerepo. Tags higher than that are transfered from monorepo"
  echo ""
  echo "Example: split-repo.sh /build/monorepo git@github.com:keboola/library-repo.git libs/my-lib my-lib/"
  exit 1
fi

SOURCE_REPO_PATH="${1}"
TARGET_REPO_URL="${2}"
LIB_PATH="${3}"
TAG_PREFIX="${4}"
LAST_TAG_IN_SINGLEREPO="${5}"

# Get absolute path
ABSOLUTE_SOURCE_PATH=$(realpath "${SOURCE_REPO_PATH}")

# Configure git to trust our repository paths
git config --system --add safe.directory "${ABSOLUTE_SOURCE_PATH}"/.git

# We require the source to be a local path because we use --mirror flag. The --mirror flag is needed on the other hand
# to copy all refs when doing a local clone.
if [[ ! -d "${ABSOLUTE_SOURCE_PATH}/.git" ]]; then
  echo "Source repo '${ABSOLUTE_SOURCE_PATH}' is not a valid GIT repository"
  exit 1
fi

TMP_DIR=`mktemp -d`
WORK_DIR=`pwd`
clean_up () {
    ARG=$?
    rm -rf $TMP_DIR
    exit $ARG
}
trap clean_up EXIT

echo ">> Cloning source repo '${ABSOLUTE_SOURCE_PATH}'"
git clone --no-local --mirror "${ABSOLUTE_SOURCE_PATH}" $TMP_DIR
cd $TMP_DIR

echo ">> Rebuild repo"
LIB_PATH="${LIB_PATH%/}/" # ensure trailing slash
git filter-repo --quiet --subdirectory-filter "${LIB_PATH}" --refname-callback "
from packaging import version

last_tag_name = b'${LAST_TAG_IN_SINGLEREPO}'
tag_prefix = b'${TAG_PREFIX}'
# copied from refname-callback.sh
## begin copy-paste
# print(b'Checking %s for prefix %s' % (refname, b'${TAG_PREFIX}'))
# not a tag -> keep as is
if not refname.startswith(b'refs/tags/'):
    return refname

tag_name = refname.decode('utf-8').split('/')[-1]
try:
    if version.parse(tag_name) <= version.parse(last_tag_name.decode('UTF-8')):
        # print('[%s] skipped' % (refname.decode('utf-8')))
        return b'refs/tags/SKIP-old-' + refname
except version.InvalidVersion:
    return b'refs/tags/SKIP-invalid-' + refname

# tag, but not matching prefix -> SKIP
if not refname.startswith(b'refs/tags/' + tag_prefix):
    # print('[%s] skipped' % (refname.decode('utf-8')))
    return b'refs/tags/SKIP-no-prefix-' + refname

# tag, with correct prefix -> strip prefix
rewritten_tag = refname[len(b'refs/tags/' + tag_prefix):]

print('[%s] rewritten to [%s]' % (refname.decode('utf-8'), rewritten_tag.decode('utf-8')))
return b'refs/tags/' + rewritten_tag
## end copy-paste
"
echo ">> Removing skipped tags"
git tag | grep '^SKIP' | xargs -I {} git tag -d {}

echo ">> Push to target repo '${TARGET_REPO_URL}'"
git remote add split "${TARGET_REPO_URL}"

# intentionally do not use --mirror to not remove existing because the repo has previously existing tags
git push -v split --all --force
git push -v split --tags --force

echo ">> Done"
