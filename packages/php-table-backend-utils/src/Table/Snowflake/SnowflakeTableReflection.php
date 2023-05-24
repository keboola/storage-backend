<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableKind;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use RuntimeException;
use Throwable;

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

    private TableKind $kind = TableKind::TABLE;

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
        /** @var array<array{TABLE_TYPE:string,BYTES:string,ROW_COUNT:string}> $row */
        $row = $this->connection->fetchAllAssociative(
            sprintf(
            //phpcs:ignore
                'SELECT TABLE_TYPE,BYTES,ROW_COUNT FROM information_schema.tables WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s;',
                SnowflakeQuote::quote($this->schemaName),
                SnowflakeQuote::quote($this->tableName)
            )
        );
        if (count($row) === 0) {
            throw TableNotExistsReflectionException::createForTable([$this->schemaName, $this->tableName]);
        }
        $this->sizeBytes = (int) $row[0]['BYTES'];
        $this->rowCount = (int) $row[0]['ROW_COUNT'];

        switch (strtoupper($row[0]['TABLE_TYPE'])) {
            case 'BASE TABLE':
                $this->isTemporary = false;
                return;
            case 'EXTERNAL TABLE':
                $this->isTemporary = false;
                $this->kind = TableKind::EXTERNAL;
                return;
            case 'LOCAL TEMPORARY':
            case 'TEMPORARY TABLE':
                $this->isTemporary = true;
                return;
            case 'VIEW':
                $this->isTemporary = false;
                $this->kind = TableKind::VIEW;
                return;
            default:
                throw new RuntimeException(sprintf(
                    'Table type "%s" is not known.',
                    $row[0]['TABLE_TYPE']
                ));
        }
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
        /** @var array<array{
         *     name: string,
         *     kind: string,
         *     type: string,
         *     default: string,
         *     'null?': string
         * }> $columnsMeta */
        $columnsMeta = $this->connection->fetchAllAssociative(
            sprintf(
                'DESC TABLE %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,])
            )
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
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,])
            )
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
        string $objectType = self::DEPENDENT_OBJECT_TABLE
    ): array {
        /** @var string $databaseName */
        $databaseName = $connection->fetchOne('SELECT CURRENT_DATABASE()');
        /** @var array<array{text:string,database_name:string,schema_name:string,name:string}> $views */
        $views = $connection->fetchAllAssociative(
            sprintf(
                'SHOW VIEWS IN DATABASE %s',
                SnowflakeQuote::quoteSingleIdentifier($databaseName)
            )
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
                            SnowflakeQuote::quote($databaseName)
                        )
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
            $this->kind
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
        return $this->kind === TableKind::VIEW;
    }
}
