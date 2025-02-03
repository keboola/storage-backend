# Table backend utils

Common stuff for table backends (Snowflake|Synapse|Redshift) shared between apps.


## Interfaces

### Database

#### Keboola\TableBackendUtils\Database\DatabaseReflectionInterface

```php
interface DatabaseReflectionInterface
{
    public function getUsersNames(?string $like = null): array;
    public function getRolesNames(?string $like = null): array;
}
```

### Schema

#### Keboola\TableBackendUtils\Schema\SchemaReflectionInterface

Function to retrieve information's about schema:
```php
interface SchemaReflectionInterface
{
    public function getTablesNames(): array;
    public function getViewsNames(): array;
}
```

### Table

#### Keboola\TableBackendUtils\Table\TableReflectionInterface

Function to retrieve information's about table:
```php
interface TableReflectionInterface
{
    public function getColumnsNames(): array;
    public function getColumnsDefinitions(): ColumnCollection;
    public function getRowsCount(): int;
    public function getPrimaryKeysNames(): array;
    public function getTableStats(): TableStatsInterface;
    public function isTemporary(): bool;
    public function getDependentViews(): array;
}
```

#### Keboola\TableBackendUtils\Table\TableQueryBuilderInterface

Queries to work with table:

```php
interface TableQueryBuilderInterface
{
    public const TIMESTAMP_COLUMN_NAME = '_timestamp';
    public function getDropTableCommand(string $schemaName, string $tableName): string;
    public function getRenameTableCommand(string $schemaName, string $sourceTableName, string $newTableName): string;
    public function getTruncateTableCommand(string $schemaName, string $tableName): string;
    public function getCreateTempTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns
    ): string;
    public function getCreateTableCommand(
        string $schemaName,
        string $tableName,
        ColumnCollection $columns,
        array $primaryKeys = []
    ): string;
    public function getCreateTableCommandFromDefinition(
        TableDefinitionInterface $definition,
        bool $definePrimaryKeys = self::CREATE_TABLE_WITHOUT_PRIMARY_KEYS
    ): string;
}
```

#### Keboola\TableBackendUtils\Table\TableStatsInterface

Table statistics

```php
interface TableStatsInterface
{
    public function getDataSizeBytes(): int;
    public function getRowsCount(): int;
}
```

### Column

#### Keboola\TableBackendUtils\Column\ColumnInterface

Table column definition:
```php
interface ColumnInterface
{
    public function getColumnName(): string;
    public function getColumnDefinition(): Keboola\Datatype\Definition\DefinitionInterface;
    public static function createGenericColumn(string $columnName): self;
}
```

### View

#### Keboola\TableBackendUtils\View\ViewReflectionInterface

Function to retrieve information's about view:
```php
interface ViewReflectionInterface
{
    public function getDependentViews(): array;
}
```

### Auth

#### Keboola\TableBackendUtils\Auth\UserReflectionInterface

```php
interface UserReflectionInterface
{
    public function endAllSessions(): void;
    public function getAllSessionIds(): array;
}
```

#### Keboola\TableBackendUtils\Auth\GrantQueryBuilderInterface

```php
interface GrantQueryBuilderInterface
{
    public function getGrantSql(GrantOptionsInterface $options): string;

    public function getRevokeSql(RevokeOptionsInterface $options): string;
}
```

## Development

### Preparation

Set up envs:
```shell
cp .env.dist .env
```

Set `TEST_PREFIX=` ENV

#### SYNAPSE

Create synapse server on Azure portal or using CLI.

set up env variables:
```bash
SYNAPSE_UID=UID
SYNAPSE_PWD=xxxx
SYNAPSE_DATABASE=synapse_db
SYNAPSE_SERVER=<synapse>.database.windows.net
```

#### Teradata

Prepare Teradata servers on AWS/Azure and set following properties. See 

create new database for tests:
```sql
CREATE DATABASE <nick>_utils_tests FROM dbc
    AS PERMANENT = 1e8,
       SPOOL = 1e8;
```

set up env variables:
```bash
TERADATA_HOST=
TERADATA_PORT=1025
TERADATA_USERNAME=
TERADATA_PASSWORD=
TERADATA_DATABASE=
```

#### AWS

In order to download TD odbc driver, create a user who can access S3 bucket with the driver package. 

set up env variables:
```bash
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```

#### Exasol

Run Exasol on your local machine in docker (for this case .env is preconfigured)
```
docker compose up -d exasol
```

Run Exasol server somewhere else and set up env variables:
```bash
EXASOL_HOST=
EXASOL_USERNAME=
EXASOL_PASSWORD=
```

_issues:_

- on slower machines Exasol can take a lot of resources. Run `docker compose -f docker compose.yml -f docker compose.limits.yml up exasol` to limit memory and cpu 

- If you are getting error `exadt::ERROR: the private interface with address '<ip>/16' either does not exist or is down.` https://github.com/exasol/docker-db/issues/45
Exasol saves you (container) current ip address and docker daemon is changing default bridge range. There are two ways to fix this:
1. each reboot set you current ip range to exasol `docket-compose run --rm exasol exaconf modify-node -n 11 -p '<ip>/16'` this is have to be done each time when ip addresses are not align.
2. set docker default bridge to some fixed range: edit or create `/etc/docker/daemon.json` and set `{"bip":"172.0.0.1/24"}` (or different range that suites you)


#### Snowflake

Prepare credentials for Snowflake access

```sql
CREATE ROLE "KEBOOLA_CI_TABLE_UTILS";
CREATE DATABASE "KEBOOLA_CI_TABLE_UTILS";

GRANT ALL PRIVILEGES ON DATABASE "KEBOOLA_CI_TABLE_UTILS" TO ROLE "KEBOOLA_CI_TABLE_UTILS";
GRANT USAGE ON WAREHOUSE "DEV" TO ROLE "KEBOOLA_CI_TABLE_UTILS";

CREATE USER "KEBOOLA_CI_TABLE_UTILS"
PASSWORD = 'my_secret_password'
DEFAULT_ROLE = "KEBOOLA_CI_TABLE_UTILS";

GRANT ROLE "KEBOOLA_CI_TABLE_UTILS" TO USER "KEBOOLA_CI_TABLE_UTILS";
```

set up env variables:

```
SNOWFLAKE_HOST=
SNOWFLAKE_PORT=443
SNOWFLAKE_USER=KEBOOLA_CI_TABLE_UTILS
SNOWFLAKE_PASSWORD='my_secret_password
SNOWFLAKE_DATABASE=KEBOOLA_CI_TABLE_UTILS
SNOWFLAKE_WAREHOUSE=
```

##### Setup snowflake Key-pair authentication
For the new key-pair authentication, which will be the only preferred method going forward, please follow these steps:
1. Open a terminal window and generate a private key and a public key using the following command:
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```
2. Assign the public key to a Snowflake user using the following command:
```sql
ALTER USER "KEBOOLA_CI_TABLE_UTILS" SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';
```
Alternatively, you can use a command that generates a query for you:
```bash
PUBLIC_KEY=$(sed '1d;$d' rsa_key.pub | tr -d '\n')
echo "ALTER USER \"KEBOOLA_CI_TABLE_UTILS\" SET RSA_PUBLIC_KEY='${PUBLIC_KEY}';"
```
3. For local tests and CI we need to edit the private key to one line and trim `-----BEGIN PRIVATE KEY----- -----END PRIVATE KEY-----` We can do this with `cat rsa_key.p8 | sed '1d;$d' | tr -d '\n'`
4. Let's save the output of the command as environment variable `SNOWFLAKE_PRIVATE_KEY` in the `.env` file or in your PhpStorm.
```bash
PRIVATE_KEY=$(sed '1d;$d' rsa_key.p8 | tr -d '\n'); if grep -q '^SNOWFLAKE_PRIVATE_KEY=' .env; then sed -i "s|^SNOWFLAKE_PRIVATE_KEY=.*|SNOWFLAKE_PRIVATE_KEY=\"$PRIVATE_KEY\"|" .env; else echo "SNOWFLAKE_PRIVATE_KEY=\"$PRIVATE_KEY\"" >> .env; fi
```

#### Bigquery

To prepare the backend you can use [Terraform template](bq-backend-init.tf).
You must have the `resourcemanager.folders.create` permission for the organization.

```bash
# run in provisioning/local/BigQuery folder
terraform init

terraform apply -var organization_id=<your-org-id> -var backend_prefix=<your_backend_prefix> -var billing_account_id=<billing_account_id>
# and enter name for your backend prefix for example your name, all resources will create with this prefx
```

After terraform apply ends go to the service project in folder created by terraform.

1. go to the newly created service project, the project id are listed at the end of the terraform call. (service_project_id)
2. click on IAM & Admin
3. on left panel choose Service Accounts
4. click on email of service account(there is only one)
5. on to the top choose Keys and Add Key => Create new key
6. select Key type JSON
7. click on the Create button and the file will automatically download
9. convert key to string`awk -v RS= '{$1=$1}1' <key_file>.json >> .env`
10. set content on last line of .env as variable `BQ_KEY_FILE`

setup envs:
```bash
BQ_KEY_FILE=<the content of the downloaded json key file>
```

### Tests

Run tests with following command.

```bash
docker compose run --rm dev composer tests
```

Unit and functional test can be run sepparetly
```bash
#unit test
docker compose run --rm dev composer tests-unit

#functional test
docker compose run --rm dev composer tests-functional
```

### Code quality check

```bash
#phplint
docker compose run --rm dev composer phplint

#phpcs
docker compose run --rm dev composer phpcs

#phpcbf
docker compose run --rm dev composer phpcbf

#phpstan
docker compose run --rm dev composer phpstan
```

### Full CI workflow

This command will run all checks load fixtures and run tests
```bash
docker compose run --rm dev composer ci
```


## Teradata connection

You can use following method to get connection to Teradata for your queries
```php
\Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection::getConnection([
            'host' => getenv('TERADATA_HOST'),
            'user' => getenv('TERADATA_USERNAME'),
            'password' => getenv('TERADATA_PASSWORD'),
            'port' => getenv('TERADATA_PORT'),
            'dbname' => '',
        ]);
```

If you want to use connection via PHPStorm DataGrip or other DB client, remove port when you setting up connection in client. Otherwise test connection will fail.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
