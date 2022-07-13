# DB Import export library

## Supported operations

- Load/Import csv from `ABS` to `Snowflake` or `Synapse`
- Unload/Export table from `Snowflake` or `Synapse` to `ABS`

## Features

### Import
- Full load - destination table is truncated before load
- Incremental load - data are merged
- Primary key dedup for all engines
- Convert empty values to NULL (using convertEmptyValuesToNull option)

## Export
- Full unload - destination csv is always rewriten

## Development

### Docker

Prepare `.env` (copy of `.env.dist`) and set up AWS keys which has access to `keboola-drivers` bucket in order to build this image.

If you don't have access to `keboola-drivers` you have to change Dockerfile.
- Comment out first stage which downloads Teradata driver and tools and supply own downloaded from Teradata site:
  - Tools: https://downloads.teradata.com/download/tools/teradata-tools-and-utilities-linux-installation-package-0
  - Driver: https://downloads.teradata.com/download/connectivity/odbc-driver/linux
  - Change `COPY --from=td` commands in Dockerfile with copy of you local Teradata packages

Then run `docker-compose build`

The AWS credentials have to also have access to bucket specified in `AWS_S3_BUCKET`. This bucket has to contain testing data. Run `docker-compose run --rm dev  composer loadS3` to load them up.


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

#### Google cloud storage

- Create bucket in [GCS](https://console.cloud.google.com/storage) set bucket name in .env variable `GCS_BUCKET_NAME`
- Create service account in [IAM](https://console.cloud.google.com/iam-admin/serviceaccounts)
- In bucket permissions grant service account admin access to bucket
- Create new service account key
- Convert key to string `awk -v RS= '{$1=$1}1' <key_file>.json >> .env`
- Set content on last line of .env as variable `GCS_CREDENTIALS`

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

-- For GCS create storage integration https://docs.snowflake.com/en/user-guide/data-load-gcs-config.html#creating-a-custom-iam-role
CREATE STORAGE INTEGRATION "KEBOOLA_DB_IMPORT_EXPORT"
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://<your gcs bucket>/');
-- set integration name to env GCS_INTEGRATION_NAME in .env file
-- get service account id `STORAGE_GCP_SERVICE_ACCOUNT`
DESC STORAGE INTEGRATION "CI_PHP_IE_LIB";
-- continue according manual ^ in snflk documentation assign roles for Data loading and unloading
```

#### SYNAPSE

Create synapse server on Azure portal or using CLI.

set up env variables:
SYNAPSE_UID
SYNAPSE_PWD
SYNAPSE_DATABASE
SYNAPSE_SERVER

Run query:
```sql
CREATE MASTER KEY;
```
this will create master key for polybase.

##### Managed Identity

Managed Identity is required when using ABS in vnet.
[docs](https://docs.microsoft.com/en-us/azure/azure-sql/database/vnet-service-endpoint-rule-overview#impact-of-using-vnet-service-endpoints-with-azure-storage)
How to setup and use Managed Identity is described in [docs](https://docs.microsoft.com/en-us/azure/synapse-analytics/sql-data-warehouse/quickstart-bulk-load-copy-tsql-examples#c-managed-identity)

> TLDR;
> In IAM of ABS add role assignment "Blob Storage Data {Reader or Contributor}" to your Synapse server principal

#### Exasol
You can run Exasol locally in Docker or you can use SaaS.

**Exasol locally in Docker**

Run Exasol on your local machine in docker (for this case .env is preconfigured)
```
docker-compose up -d exasol
```

Run Exasol server somewhere else and set up env variables:
```bash
EXASOL_HOST=
EXASOL_USERNAME=
EXASOL_PASSWORD=
```

**Exasol in SaaS**

Login to SaaS UI (or use a local client) and create user with following grants.
```SQL

CREATE USER "<nick>_ie" IDENTIFIED BY "password";

GRANT 
CREATE SESSION,
CREATE SCHEMA,
CREATE TABLE,
CREATE VIEW,
CREATE USER,
CREATE ROLE,
DROP USER,
DROP ANY ROLE,
GRANT ANY ROLE,
ALTER ANY SCHEMA,
ALTER USER,
IMPORT,
EXPORT
TO "<nick>_ie"
WITH ADMIN OPTION;
```

Obtain host (with port), username and password (from previous step) and fill it in `.env` as desribed above. Make sure, that your account has enabled network for your IP.

#### Teradata

Prepare Teradata servers on AWS/Azure and set following properties. See

create new database for tests:
```sql
CREATE DATABASE <nick>_ie_lib_tests FROM dbc
    AS PERMANENT = 1e8,
       SPOOL = 1e8;
```

```bash
TERADATA_HOST=
TERADATA_USERNAME=
TERADATA_PASSWORD=
TERADATA_PORT=
TERADATA_DATABASE=
```


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

#### Snowflake

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

#### Synapse next (experimental)

Import to Synapse

```php
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Doctrine\DBAL\Connection;

$importSource = new Storage\ABS\SourceFile(...);
// or
$importSource = new Storage\Synapse\Table(...);
// or
$importSource = new Storage\Synapse\SelectSource(...);

$destinationTable = new SynapseTableDefinition(...);
$options = new SynapseImportOptions(...);
$synapseConnection = new Connection(...);

$stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
    $destinationTable,
    $importSource->getColumnsNames()
);
$qb = new SynapseTableQueryBuilder($synapseConnection);
$synapseConnection->executeStatement(
    $qb->getCreateTableCommandFromDefinition($stagingTable)
);
$toStageImporter = new ToStageImporter($synapseConnection);
$toFinalTableImporter = new IncrementalImporter($synapseConnection);
// or
$toFinalTableImporter = new FullImporter($synapseConnection);
try {
    $importState = $toStageImporter->importToStagingTable(
        $importSource,
        $stagingTable,
        $options
    );
    $result = $toFinalTableImporter->importToTable(
        $stagingTable,
        $destinationTable,
        $options,
        $importState
    );
} finally {
    $synapseConnection->executeStatement(
        (new SqlBuilder())->getDropTableIfExistsCommand(
            $stagingTable->getSchemaName(),
            $stagingTable->getTableName()
        )
    );    
}

```

### Internals/Extending

Library consists of few simple interfaces.

#### Create new backend

Importer, Exporter Interface must be implemented in new Backed
```
Keboola\Db\ImportExport\Backend\ImporterInterface
Keboola\Db\ImportExport\Backend\ExporterInterface
```

For each backend there is corresponding adapter which supports own combination of SourceInterface and DestinationInterface. Custom adapters can be set with `setAdapters` method.

#### Create new storage

Storage is now file storage ABS|S3 (in future) or table storage Snowflake|Synapse.
Storage can have `Source` and `Destination` which must implement `SourceInterface` or `DestinationInterface`. These interfaces are empty and it's up to adapter to support own combination.
In general there is one Import/Export adapter per FileStorage <=> TableStorage combination.

Adapter must implement:
- `Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface` for import
- `Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface` for export

Backend can require own extended AdapterInterface (Synapse and Snowflake do now).



## License

MIT licensed, see [LICENSE](./LICENSE) file.
