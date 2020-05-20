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

#### SYNAPSE

Create synapse server on Azure portal or using CLI.

set up env variables:
```bash
SYNAPSE_UID=UID
SYNAPSE_PWD=xxxx
SYNAPSE_DATABASE=synapse_db
SYNAPSE_SERVER=<synapse>.database.windows.net
```

### Tests

Run tests with following command.

```bash
docker-compose run --rm dev composer tests
```

Unit and functional test can be run sepparetly
```bash
#unit test
docker-compose run --rm dev composer tests-unit

#functional test
docker-compose run --rm dev composer tests-functional
```

### Code quality check

```bash
#phplint
docker-compose run --rm dev composer phplint

#phpcs
docker-compose run --rm dev composer phpcs

#phpstan
docker-compose run --rm dev composer phpstan
```

### Full CI workflow

This command will run all checks load fixtures and run tests
```bash
docker-compose run --rm dev composer ci
```
