<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;
use Keboola\TableBackendUtils\Table\TableType;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use RuntimeException;
use Throwable;

/**
 * @phpstan-type SHOW_TABLE_ROW array{name:string,kind:string,bytes:string,rows:string,is_external:'Y'|'N'}
 * @phpstan-type SHOW_VIEW_ROW array{name:string,kind:string}
 */
final class SnowflakeTableReflection implements TableReflectionInterface
{
    public const DEPENDENT_OBJECT_TABLE = 'TABLE';
    public const DEPENDENT_OBJECT_VIEW = 'VIEW';
    public const COLUMN_KIND_COLUMN = 'COLUMN';
    public const COLUMN_KIND_VIRTUAL = 'VIRTUAL';
    public const RECOGNIZED_COLUMN_KINDS = [
        self::COLUMN_KIND_COLUMN,
        self::COLUMN_KIND_VIRTUAL,
    ];

    private Connection $connection;

    private string $schemaName;

    private string $tableName;

    private ?bool $isTemporary = null;

    private TableType $tableType = TableType::TABLE;

    private ?int $sizeBytes = null;

    private ?int $rowCount = null;

    public function __construct(Connection $connection, string $schemaName, string $tableName)
    {
        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @throws TableNotExistsReflectionException
     */
    private function cacheTableProps(bool $force = false): void
    {
        if ($force === false && $this->isTemporary !== null) {
            return;
        }

        try {
            /** @var array<SHOW_TABLE_ROW> $rows */
            $rows = $this->connection->fetchAllAssociative(
                sprintf(
                //phpcs:ignore
                    'SHOW TABLES LIKE %s IN SCHEMA %s',
                    SnowflakeQuote::quote($this->tableName),
                    SnowflakeQuote::quoteSingleIdentifier($this->schemaName),
                ),
            );// show tables is case insensitive on table names so we need to check if we got any exact match
            /** @var SHOW_TABLE_ROW|null $table */
            $table = $this->getTableByNameFromShow($rows);
            if ($table === null) {
                // if table is actually a view, fetch view info
                /** @var array<SHOW_VIEW_ROW> $rows */
                $rows = $this->connection->fetchAllAssociative(
                    sprintf(
                    //phpcs:ignore
                        'SHOW VIEWS LIKE %s IN SCHEMA %s',
                        SnowflakeQuote::quote($this->tableName),
                        SnowflakeQuote::quoteSingleIdentifier($this->schemaName),
                    ),
                );
                $view = $this->getTableByNameFromShow($rows);
                if ($view === null) {
                    throw TableNotExistsReflectionException::createForTable([$this->schemaName, $this->tableName]);
                }
                $this->sizeBytes = 0;
                $this->rowCount = 0;
                $this->isTemporary = false;
                $this->tableType = TableType::VIEW;
                return;
            }
        } catch (Throwable $e) {
            if ($e instanceof TableNotExistsReflectionException) {
                throw $e;
            }
            if (str_contains($e->getMessage(), 'Cannot access object or it does not exist')) {
                throw TableNotExistsReflectionException::createForTable([$this->schemaName, $this->tableName], $e);
            }

            throw $e;
        }

        $this->sizeBytes = (int) $table['bytes'];
        $this->rowCount = (int) $table['rows'];

        if (array_key_exists('is_external', $table) && $table['is_external'] === 'Y') {
            $this->isTemporary = false;
            $this->tableType = TableType::SNOWFLAKE_EXTERNAL;
            return;
        }

        switch (strtoupper($table['kind'])) {
            case 'BASE TABLE':
            case 'TABLE':
                $this->isTemporary = false;
                return;
            case 'EXTERNAL TABLE':
                $this->isTemporary = false;
                $this->tableType = TableType::SNOWFLAKE_EXTERNAL;
                return;
            case 'TEMPORARY':
            case 'LOCAL TEMPORARY':
            case 'TEMPORARY TABLE':
                $this->isTemporary = true;
                return;
            case 'VIEW':
            case 'MATERIALIZED_VIEW':
                $this->isTemporary = false;
                $this->tableType = TableType::VIEW;
                return;
            default:
                throw new RuntimeException(sprintf(
                    'Table type "%s" is not known.',
                    $table['kind'],
                ));
        }
    }

    /**
     * @param array<SHOW_TABLE_ROW|SHOW_VIEW_ROW> $rows
     * @return SHOW_TABLE_ROW|SHOW_VIEW_ROW|null
     */
    private function getTableByNameFromShow(array $rows): ?array
    {
        foreach ($rows as $row) {
            if ($row['name'] === $this->tableName) {
                return $row;
            }
        }
        return null;
    }

    /**
     * @return string[]
     * @throws TableNotExistsReflectionException
     */
    public function getColumnsNames(): array
    {
        $columns = $this->getColumnsDefinitions();

        $names = [];
        /** @var SnowflakeColumn $col */
        foreach ($columns as $col) {
            $names[] = $col->getColumnName();
        }
        return $names;
    }

    /**
     * @throws TableNotExistsReflectionException
     */
    public function getColumnsDefinitions(): ColumnCollection
    {
        $this->cacheTableProps();

        /**
         * @var array<array{
         *     name: string,
         *     kind: string,
         *     type: string,
         *     default: string,
         *     "null?": string,
         * }> $columnsMeta
         */
        $columnsMeta = $this->connection->fetchAllAssociative(
            sprintf(
                'DESC TABLE %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,]),
            ),
        );

        $columns = [];

        foreach ($columnsMeta as $col) {
            if (in_array($col['kind'], self::RECOGNIZED_COLUMN_KINDS)) {
                $columns[] = SnowflakeColumn::createFromDB($col);
            }
        }

        return new ColumnCollection($columns);
    }

    /**
     * @throws TableNotExistsReflectionException
     */
    public function getRowsCount(): int
    {
        $this->cacheTableProps(true);
        assert($this->rowCount !== null);
        return $this->rowCount;
    }

    /**
     * returns list of column names where PK is defined on
     *
     * @return string[]
     * @throws TableNotExistsReflectionException
     */
    public function getPrimaryKeysNames(): array
    {
        $this->cacheTableProps();
        /** @var array<array{column_name:string}> $columnsMeta */
        $columnsMeta = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW PRIMARY KEYS IN TABLE %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,]),
            ),
        );

        return array_map(fn($pkRow) => $pkRow['column_name'], $columnsMeta);
    }

    /**
     * @throws TableNotExistsReflectionException
     */
    public function getTableStats(): TableStatsInterface
    {
        $this->cacheTableProps(true);
        assert($this->sizeBytes !== null);
        assert($this->rowCount !== null);
        return new TableStats($this->sizeBytes, $this->rowCount);
    }

    /**
     * @throws TableNotExistsReflectionException
     */
    public function isTemporary(): bool
    {
        $this->cacheTableProps();
        assert($this->isTemporary !== null);
        return $this->isTemporary;
    }

    /**
     * @phpstan-impure
     * @return array<int, array<string, mixed>>
     * array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public static function getDependentViewsForObject(
        Connection $connection,
        string $objectName,
        string $schemaName,
        string $objectType = self::DEPENDENT_OBJECT_TABLE,
    ): array {
        /** @var string $databaseName */
        $databaseName = $connection->fetchOne('SELECT CURRENT_DATABASE()');
        /** @var array<array{text:string,database_name:string,schema_name:string,name:string}> $views */
        $views = $connection->fetchAllAssociative(
            sprintf(
                'SHOW VIEWS IN DATABASE %s',
                SnowflakeQuote::quoteSingleIdentifier($databaseName),
            ),
        );

        $dependentViews = [];
        foreach ($views as $viewRow) {
            // check that the tableName exists in DDL of the view
            if (preg_match('/.*' . $objectName . '.*/i', $viewRow['text']) === 1) {
                try {
                    $dependentObjects = $connection->fetchAllAssociative(
                        sprintf(
                            '
SELECT * FROM TABLE(get_object_references(database_name=>%s, SCHEMA_NAME=>%s, object_name=>%s))  
WHERE REFERENCED_OBJECT_TYPE = %s 
  AND REFERENCED_OBJECT_NAME = %s
  AND REFERENCED_SCHEMA_NAME = %s
  AND REFERENCED_DATABASE_NAME = %s
  ',
                            SnowflakeQuote::quoteSingleIdentifier($viewRow['database_name']),
                            SnowflakeQuote::quoteSingleIdentifier($viewRow['schema_name']),
                            SnowflakeQuote::quoteSingleIdentifier($viewRow['name']),
                            SnowflakeQuote::quote($objectType),
                            SnowflakeQuote::quote($objectName),
                            SnowflakeQuote::quote($schemaName),
                            SnowflakeQuote::quote($databaseName),
                        ),
                    );

                    if ($dependentObjects !== []) {
                        $dependentViews[] = [
                            'schema_name' => $viewRow['schema_name'],
                            'name' => $viewRow['name'],
                        ];
                    }
                } catch (Throwable $e) {
                    continue;
                }
            }
        }
        return $dependentViews;
    }

    /**
     * @phpstan-impure
     * @return array<int, array<string, mixed>>
     * array{
     *  schema_name: string,
     *  name: string
     * }[]
     * @throws TableNotExistsReflectionException
     */
    public function getDependentViews(): array
    {
        $this->cacheTableProps();
        return self::getDependentViewsForObject($this->connection, $this->tableName, $this->schemaName, 'TABLE');
    }

    /**
     * @throws TableNotExistsReflectionException
     */
    public function getTableDefinition(): TableDefinitionInterface
    {
        $this->cacheTableProps();
        return new SnowflakeTableDefinition(
            $this->schemaName,
            $this->tableName,
            $this->isTemporary(),
            $this->getColumnsDefinitions(),
            $this->getPrimaryKeysNames(),
            $this->tableType,
        );
    }

    public function exists(): bool
    {
        try {
            $this->cacheTableProps(true);
        } catch (TableNotExistsReflectionException $e) {
            return false;
        }

        return true;
    }

    public function isView(): bool
    {
        $this->cacheTableProps();
        return $this->tableType === TableType::VIEW;
    }
}
