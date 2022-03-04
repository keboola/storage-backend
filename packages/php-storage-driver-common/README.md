# Keboola Storage Driver Teradata

Keboola high level storage backend driver for Teradata

## Setup Teradata

```SQL
-- set session database of your choice
SET SESSION DATABASE <your root database>;

-- create root database for driver, use memory allocation of you choice 
CREATE DATABASE <nickname>_driver_teradata FROM <your root database>
    AS PERMANENT = 1e9,
       SPOOL = 1e9;

-- grant rights
GRANT CREATE DATABASE, DROP DATABASE, CREATE USER, DROP USER ON <nickname>_driver_teradata TO <nickname>_driver_teradata;
GRANT CREATE ROLE, DROP ROLE TO <nickname>_driver_teradata;
```

setup envs:
```
TERADATA_HOST=
TERADATA_USERNAME=
TERADATA_PASSWORD=
TERADATA_PORT=1025
TERADATA_ROOT_DATABASE=<nickname>_driver_teradata
```

## Setup AWS credentials

You access key must have access to `keboola-drivers` bucket to download Teradata odbc driver and utilities
```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
# bucket is needed for import/export tests
AWS_S3_BUCKET=
AWS_REGION=eu-central-1
```

## Build docker images

```bash
docker-compose build
```

## Xdebug

To run with xdebug use `dev-xdebug` container instead of `dev`

## Generate protobuf code

```bash
docker-compose run --rm dev composer protobuf
```

### Generate protobuf documentation

```bash
docker run --rm \
  -v $(pwd)/docs:/out \
  -v $(pwd)/proto:/protos \
  pseudomuto/protoc-gen-doc
```

## Tests

Run tests with following command.

```bash
docker-compose run --rm dev composer tests
```

To run separate tests first generate testing protobuf classes
```bash
docker-compose run --rm dev composer protobufTests
```
And than run phpunit
```bash
docker-compose run --rm dev composer phpunit -- --filter=TeradataDriverClientTest
```

## Code quality check

```bash
#run all bellow but not tests
docker-compose run --rm dev composer check

#phplint
docker-compose run --rm dev composer phplint

#phpcs
docker-compose run --rm dev composer phpcs

#phpcbf
docker-compose run --rm dev composer phpcbf

#phpstan
docker-compose run --rm dev composer phpstan
```

## Full CI workflow

This command will run all checks and run tests
```bash
docker-compose run --rm dev composer ci
```
