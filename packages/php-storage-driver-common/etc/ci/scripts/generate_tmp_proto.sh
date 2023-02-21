#!/bin/bash

cd /code
mkdir tmp_generated
FILES=$(find proto -iname "*.proto")
protoc $FILES --php_out=tmp_generated
