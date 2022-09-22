<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;
use Throwable;

final class SnowflakeTableReflection implements TableReflectionInterface
{
    public const DEPENDENT_OBJECT_TABLE = 'TABLE';
    public const DEPENDENT_OBJECT_VIEW = 'VIEW';

    private Connection $connection;

    private string $schemaName;

    private string $tableName;

    private ?bool $isTemporary = null;

    public function __construct(Connection $connection, string $schemaName, string $tableName)
    {
        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    private function setIsTemporary(): bool
    {
        $row = $this->connection->fetchAssociative(
            sprintf(
                // STARTS WITH is added because it is case-sensitive
                'SHOW TABLES LIKE %s IN %s STARTS WITH %s ',
                SnowflakeQuote::quote($this->tableName),
                SnowflakeQuote::quoteSingleIdentifier($this->schemaName),
                SnowflakeQuote::quote($this->tableName)
            )
        );

        if ($row) {
            return $row['kind'] === 'TEMPORARY';
        }

        throw new TableNotExistsReflectionException('Cannot detect if table is temporary or not. Table does not exist');
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        /** @var array<array{column_name: string}> $columnsData */
        $columnsData = $this->connection->fetchAllAssociative(
            sprintf(
                // case-sensitive
                'SHOW COLUMNS IN %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,])
            )
        );

        return array_values(array_map(fn($column) => $column['column_name'], $columnsData));
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
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
            if ($col['kind'] === 'COLUMN') {
                $columns[] = SnowflakeColumn::createFromDB($col);
            }
        }

        return new ColumnCollection($columns);
    }



    public function getRowsCount(): int
    {
        /** @var int|string $result */
        $result = $this->connection->fetchOne(sprintf(
            'SELECT COUNT(*) AS NumberOfRows FROM %s',
            SnowflakeQuote::createQuotedIdentifierFromParts([
                $this->schemaName,
                $this->tableName,
            ])
        ));
        return (int) $result;
    }

    /**
     * returns list of column names where PK is defined on
     *
     * @return string[]
     */
    public function getPrimaryKeysNames(): array
    {
        /** @var array<array{column_name:string}> $columnsMeta */
        $columnsMeta = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW PRIMARY KEYS IN TABLE %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,])
            )
        );

        return array_map(fn($pkRow) => $pkRow['column_name'], $columnsMeta);
    }

    public function getTableStats(): TableStatsInterface
    {
        $sql = sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s STARTS WITH %s',
            ExasolQuote::quote($this->tableName),
            ExasolQuote::quoteSingleIdentifier($this->schemaName),
            ExasolQuote::quote($this->tableName)
        );
        /** @var array{bytes:int|string}|null $result */
        $result = $this->connection->fetchAssociative($sql);
        if (!$result) {
            throw new TableNotExistsReflectionException('Table does not exist');
        }

        return new TableStats((int) $result['bytes'], $this->getRowsCount());
    }

    public function isTemporary(): bool
    {
        if ($this->isTemporary === null) {
            $this->isTemporary = $this->setIsTemporary();
        }
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
     */
    public function getDependentViews(): array
    {
        return self::getDependentViewsForObject($this->connection, $this->tableName, $this->schemaName, 'TABLE');
    }

    public function getTableDefinition(): TableDefinitionInterface
    {
        return new SnowflakeTableDefinition(
            $this->schemaName,
            $this->tableName,
            $this->isTemporary(),
            $this->getColumnsDefinitions(),
            $this->getPrimaryKeysNames()
        );
    }

    public function exists(): bool
    {
        $row = $this->connection->fetchAssociative(
            sprintf(
                "SELECT *
FROM information_schema.tables 
WHERE TABLE_TYPE = 'BASE TABLE'
AND TABLE_NAME = %s AND TABLE_SCHEMA = %s
",
                SnowflakeQuote::quote($this->tableName),
                SnowflakeQuote::quote($this->schemaName),
            )
        );

        return $row !== false;
    }
}
