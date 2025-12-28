# DB Import export library

## Supported operations

- Load/Import csv from `ABS` to `Snowflake`
- Load/Import csv from `GCS` to `Bigquery`
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

### Docker

Prepare `.env` (copy of `.env.dist`) and set up AWS keys which has access to `keboola-drivers` bucket in order to build this image. Also add this user to group `ci-php-import-export-lib` witch will allow you to work with newly created bucket for tests.

User can be created in `Dev - Main legacy`, where are also groups for `keboola-drivers` and `ci-php-import-export-lib`.

Then run `docker compose build`

The AWS credentials have to also have access to bucket specified in `AWS_S3_BUCKET`. This bucket has to contain testing data. Run `docker compose run --rm dev  composer loadS3` to load them up.


### Preparation

#### AWS

Run terraform to create resources for tests. See [Terraform.md](./docs/Terraform.md) for details.


#### Azure

- Create [storage account](https://portal.azure.com/#create/Microsoft.StorageAccount-ARM) template can be found in provisioning ABS [create template](https://portal.azure.com/#create/Microsoft.Template)
- Create container in storage account `Blob service -> Containers` *note: for tests this step can be skiped container is created with `loadAbs` cmd*
- Fill env variables in .env file
```
ABS_ACCOUNT_NAME=storageAccount
ABS_ACCOUNT_KEY=accountKey
ABS_CONTAINER_NAME=containerName
```
- Upload test fixtures to ABS `docker compose run --rm dev composer loadAbs`

#### Google cloud storage

- Create bucket in [GCS](https://console.cloud.google.com/storage) set bucket name in .env variable `GCS_BUCKET_NAME`
- Create service account in [IAM](https://console.cloud.google.com/iam-admin/serviceaccounts)
- In bucket permissions grant service account admin access to bucket
- Create new service account key
- Convert key to string `awk -v RS= '{$1=$1}1' <key_file>.json >> .env` (or `cat file.json | jq -c | jq -R`)
- Set content on last line of .env as variable `GCS_CREDENTIALS`

- Upload test fixtures to GCS `docker compose run --rm dev composer loadGcs-bigquery` or `docker compose run --rm dev composer loadGcs-snowflake` (depending on backend)

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
DESC STORAGE INTEGRATION "KEBOOLA_DB_IMPORT_EXPORT";
-- continue according manual ^ in snflk documentation assign roles for Data loading and unloading
```

#### Bigquery
Install [Google Cloud client](https://cloud.google.com/sdk/docs/install-sdk) (via [Brew](https://formulae.brew.sh/cask/google-cloud-sdk#default)), initialize it
and log in to [generate default credentials](https://cloud.google.com/docs/authentication/application-default-credentials#personal).

To prepare the backend you can use [Terraform template](./bq-storage-backend-init.tf).
You must have the `resourcemanager.folders.create` permission for the organization.
```bash
# you can copy it to a folder somewhere and make an init
terraform init
```

Run `terraform apply` with following variables:
 - folder_id: Go to [GCP Resource Manager](https://console.cloud.google.com/cloud-resource-manager) and select your team dev folder ID (e.g. find 'KBC Team Dev' and copy ID)
 - backend_prefix: your_name, all resources will create with this prefix
 - billing_account_id: Go to [Billing](https://console.cloud.google.com/billing/) and copy your Billing account ID

```bash
terraform apply -var folder_id=<folder_id> -var backend_prefix=<your_prefix> -var billing_account_id=<billing_account_id>
```

For missing pieces see [Connection repository](https://github.com/keboola/connection/blob/master/DOCKER.md#bigquery).
After terraform apply ends go to the service project in folder created by terraform.
1. convert key to string and save to `.env` file: `awk -v RS= '{$1=$1}1' <key_file>.json >> .env`
2. set content on the last line of `.env` as variable `BQ_KEY_FILE`
3. set env variable `BQ_BUCKET_NAME` generated from TF template `file_storage_bucket_id`

### Tests

Run tests with following command.

*note: azure credentials must be provided and fixtures uploaded*

```
docker compose run --rm dev composer tests
```

Unit and functional test can be run sepparetly
```
#unit test
docker compose run --rm dev composer tests-unit

#functional test
docker compose run --rm dev composer tests-functional
```

### Code quality check

```
#phplint
docker compose run --rm dev composer phplint

#phpcs
docker compose run --rm dev composer phpcs

#phpstan
docker compose run --rm dev composer phpstan
```

### Full CI workflow

This command will run all checks load fixtures and run tests
```
docker compose run --rm dev composer ci
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

Storage is now file storage ABS|S3 (in future) or table storage Snowflake.
Storage can have `Source` and `Destination` which must implement `SourceInterface` or `DestinationInterface`. These interfaces are empty and it's up to adapter to support own combination.
In general there is one Import/Export adapter per FileStorage <=> TableStorage combination.

Adapter must implement:
- `Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface` for import
- `Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface` for export

Backend can require own extended AdapterInterface.



## License

MIT licensed, see [LICENSE](./LICENSE) file.
