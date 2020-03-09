# DB Import export library

## Supported operations

- Load/Import csv from `ABS` to `Snowflake` or `Synapse` 
- Unload/Export table from `Snowflake` to `ABS` 

## Features

### Import
- Full load - destination table is truncated before load
- Incremental load - data are merged
- Primary key dedup for all engines
- Convert empty values to NULL (using convertEmptyValuesToNull option)

## Export
- Full unload - destination csv is always rewriten

## Development

### Preparation

#### Azure

- Create [storage account](https://portal.azure.com/#create/Microsoft.StorageAccount-ARM) template can be found in provisioning ABS [create template](https://portal.azure.com/#create/Microsoft.Template)
- Create container in storage account `Blob service -> Containers` *note: for tests this step can be skiped container is created with `loadAbs` cmd*
- Fill env variables in .env file
```
ABS_ACCOUNT_NAME=storageAccount
ABS_ACCOUNT_KEY=accountKey
ABS_CONTAINER_NAME=containerName
```
- Upload test fixtures to ABS `docker-compose run --rm dev composer loadAbs`

#### SNOWFLAKE

Role, user, database and warehouse are required for tests. You can create them:

```sql 
CREATE ROLE "KEBOOLA_DB_IMPORT_EXPORT";
CREATE DATABASE "KEBOOLA_DB_IMPORT_EXPORT";

GRANT ALL PRIVILEGES ON DATABASE "KEBOOLA_DB_IMPORT_EXPORT" TO ROLE "KEBOOLA_DB_IMPORT_EXPORT";
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "KEBOOLA_DB_IMPORT_EXPORT";

CREATE USER "KEBOOLA_DB_IMPORT_EXPORT"
PASSWORD = 'Password'
DEFAULT_ROLE = "KEBOOLA_DB_IMPORT_EXPORT";

GRANT ROLE "KEBOOLA_DB_IMPORT_EXPORT" TO USER "KEBOOLA_DB_IMPORT_EXPORT";
```

#### SYNAPSE

Create synapse server on Azure portal or using CLI.

set up env variables:
SYNAPSE_UID
SYNAPSE_PWD
SYNAPSE_DATABASE
SYNAPSE_SERVER

### Tests

Run tests with following command.

*note: azure credentials must be provided and fixtures uploaded*
 
```
docker-compose run --rm dev composer tests
```

Unit and functional test can be run sepparetly
```
#unit test
docker-compose run --rm dev composer tests-unit

#functional test
docker-compose run --rm dev composer tests-functional
```

### Code quality check

```
#phplint
docker-compose run --rm dev composer phplint

#phpcs
docker-compose run --rm dev composer phpcs

#phpstan
docker-compose run --rm dev composer phpstan
```

### Full CI workflow

This command will run all checks load fixtures and run tests
```
docker-compose run --rm dev composer ci
```


### Usage

ABS -> Snowflake `import/load`
```php
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

$absSourceFile = new Storage\ABS\SourceFile(...);
$snowflakeDestinationTable = new Storage\Snowflake\Table(...);
$importOptions = new ImportOptions(...);

(new Importer($snowflakeConnection))->importTable(
    $absSourceFile,
    $snowflakeDestinationTable,
    $importOptions
);
```

Snowflake -> Snowflake `copy`
```php
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

$snowflakeSourceTable = new Storage\Snowflake\Table(...);
$snowflakeDestinationTable = new Storage\Snowflake\Table(...);
$importOptions = new ImportOptions(...);

(new Importer($snowflakeConnection))->importTable(
    $snowflakeSourceTable,
    $snowflakeDestinationTable,
    $importOptions
);
```

Snowflake -> ABS `export/unload`
```php
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

$snowflakeSourceTable = new Storage\Snowflake\Table(...);
$absDestinationFile = new Storage\ABS\DestinationFile(...);
$exportOptions = new ExportOptions(...);

(new Exporter($snowflakeConnection))->exportTable(
    $snowflakeSourceTable,
    $absDestinationFile,
    $exportOptions
);
```

### Internals/Extending

Library consists of few simple interfaces.

#### Create new backend

Importer, Exporter Interface must be implemented in new Backed
```
Keboola\Db\ImportExport\Backend\ImporterInterface
Keboola\Db\ImportExport\Backend\ExporterInterface
```

For each backend there is corresponding adapter injected by Source or Destination.

#### Create new Storage

Storage can have `Source` and `Destination` which must implement `SourceInterface` or `DestinationInterface`.

- **SourceInterface** has method `getBackendImportAdapter` which must return Adapter implementing `BackendImportAdapterInterface` for used Backend   
- **DestinationInterface** has method `getBackendExportAdapter` which must return Adapter implementing `BackendExportAdapterInterface` for used Backend


