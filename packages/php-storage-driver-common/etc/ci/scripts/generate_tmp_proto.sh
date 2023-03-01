#!/bin/bash

cd /code/packages/php-storage-driver-common
mkdir tmp_generated
FILES=$(find proto -iname "*.proto")
protoc $FILES --php_out=tmp_generated
