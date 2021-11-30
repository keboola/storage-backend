<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Snowflake;

use Doctrine\DBAL\Connection;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
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

    /** @var string */
    private $dbName;

    public function __construct(Connection $connection, string $dbName, string $schemaName, string $tableName)
    {
        $this->isTemporary = false;

        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->dbName = $dbName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        // TODO
    }

    public function getColumnsDefinitions(): ColumnCollection
    {

        $this->connection->executeQuery(sprintf('USE SCHEMA %s', SnowflakeQuote::createQuotedIdentifierFromParts([
            $this->dbName,
            $this->schemaName
        ])));

        $columnsMeta = $this->connection->fetchAllAssociative(sprintf('DESC TABLE %s', SnowflakeQuote::createQuotedIdentifierFromParts([
            $this->dbName,
            $this->schemaName,
            $this->tableName,
        ])));

        $columns = [];

        foreach ($columnsMeta as $col){
            if ($col['kind'] === 'COLUMN') {
                $columns[] = SnowflakeColumn::createFromDB($col);
            }
        }

        return new ColumnCollection($columns);
    }



    public function getRowsCount(): int
    {
        // TODO
    }

    /**
     * returns list of column names where PK is defined on
     *
     * @return string[]
     */
    public function getPrimaryKeysNames(): array
    {
        // TODO
    }

    public function getTableStats(): TableStatsInterface
    {
        // TODO
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
        // TODO
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
