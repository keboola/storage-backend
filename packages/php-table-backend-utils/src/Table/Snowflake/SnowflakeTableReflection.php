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

final class SnowflakeTableReflection implements TableReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $schemaName;

    /** @var string */
    private $tableName;

    /** @var bool */
    private $isTemporary;

    public function __construct(Connection $connection, string $schemaName, string $tableName)
    {
        $this->isTemporary = strpos($tableName, SnowflakeTableQueryBuilder::TEMP_TABLE_PREFIX) === 0;

        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        $columnsData = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW COLUMNS IN %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,])
            )
        );

        return array_values(array_map(function ($column) {
            return $column['column_name'];
        }, $columnsData));
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        $this->connection->executeQuery(sprintf('USE SCHEMA %s', SnowflakeQuote::createQuotedIdentifierFromParts([
            $this->schemaName,
        ])));

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
        $columnsMeta = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW PRIMARY KEYS IN TABLE %s',
                SnowflakeQuote::createQuotedIdentifierFromParts([$this->schemaName, $this->tableName,])
            )
        );

        return array_map(function ($pkRow) {
            return $pkRow['column_name'];
        }, $columnsMeta);
    }

    public function getTableStats(): TableStatsInterface
    {
        $sql = sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s',
            ExasolQuote::quote($this->tableName),
            ExasolQuote::quoteSingleIdentifier($this->schemaName)
        );
        $result = $this->connection->fetchAssociative($sql);
        $bytes = $result ? (int) $result['bytes'] : 0;
        return new TableStats($bytes, $this->getRowsCount());
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
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
        $databaseName = $this->connection->fetchOne('SELECT CURRENT_DATABASE()');
        $views = $this->connection->fetchAllAssociative(
            sprintf(
                'SHOW VIEWS IN DATABASE %s',
                SnowflakeQuote::quoteSingleIdentifier($databaseName)
            )
        );

        $dependentViews = [];
        foreach ($views as $viewRow) {
            // check that the tableName exists in DDL of the view
            if (preg_match('/.*' . $this->tableName . '.*/i', $viewRow['text']) === 1) {
                try {
                    $dependentObjects = $this->connection->fetchAllAssociative(
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
                            SnowflakeQuote::quote('TABLE'),
                            SnowflakeQuote::quote($this->tableName),
                            SnowflakeQuote::quote($this->schemaName),
                            SnowflakeQuote::quote($databaseName)
                        )
                    );

                    if (count($dependentObjects)) {
                        $dependentViews[] = [
                            'schema_name' => $viewRow['schema_name'],
                            'name' => $viewRow['name'],
                        ];
                    }
                } catch (\Throwable $e) {
                    continue;
                }
            }
        }
        return $dependentViews;
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
}
