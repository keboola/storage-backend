<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;

final class ExasolTableReflection implements TableReflectionInterface
{
    private Connection $connection;

    private string $schemaName;

    private string $tableName;

    private bool $isTemporary = false;

    public function __construct(Connection $connection, string $schemaName, string $tableName)
    {
        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        /** @var array<array{COLUMN_NAME:string}> $columns */
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                'DESCRIBE %s.%s',
                ExasolQuote::quoteSingleIdentifier($this->schemaName),
                ExasolQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        return array_map(static fn($column) => $column['COLUMN_NAME'], $columns);
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        /** @var array<array{
         *  COLUMN_DEFAULT: mixed,
         *  COLUMN_NAME: string,
         *  TYPE_NAME: string,
         *  COLUMN_IS_NULLABLE: string,
         *  COLUMN_TYPE: string,
         *  TYPE_NAME: string,
         *  COLUMN_NUM_PREC: ?string,
         *  COLUMN_NUM_SCALE: ?string,
         *  COLUMN_MAXSIZE: ?string,
         * }> $columns */
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                '
SELECT 
    "COLUMN_NAME", 
    "COLUMN_IS_NULLABLE", 
    "COLUMN_MAXSIZE", 
    "COLUMN_NUM_PREC", 
    "COLUMN_NUM_SCALE", 
    "COLUMN_DEFAULT", 
    "COLUMN_TYPE",
    "TYPES"."TYPE_NAME"
FROM "SYS"."EXA_ALL_COLUMNS" "COLUMNS"
JOIN "SYS"."EXA_SQL_TYPES" "TYPES" ON "COLUMNS"."COLUMN_TYPE_ID" = "TYPES"."TYPE_ID"
WHERE "COLUMN_SCHEMA" = %s
  AND "COLUMN_TABLE" =  %s
ORDER BY "COLUMN_ORDINAL_POSITION"
  ',
                ExasolQuote::quote($this->schemaName),
                ExasolQuote::quote($this->tableName)
            )
        );

        $columns = array_map(static fn($col) => ExasolColumn::createFromDB($col), $columns);

        return new ColumnCollection($columns);
    }

    public function getRowsCount(): int
    {
        /** @var int|string $result */
        $result = $this->connection->fetchOne(sprintf(
            'SELECT COUNT(*) AS NumberOfRows FROM %s.%s',
            ExasolQuote::quoteSingleIdentifier($this->schemaName),
            ExasolQuote::quoteSingleIdentifier($this->tableName)
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
        $sql = sprintf(
            '
  SELECT "COLUMN_NAME"
  FROM "SYS"."EXA_ALL_CONSTRAINT_COLUMNS"
  WHERE "CONSTRAINT_SCHEMA" = %s 
    AND "CONSTRAINT_TABLE" = %s 
    AND "CONSTRAINT_TYPE" = %s
  ORDER BY "ORDINAL_POSITION"
            ',
            ExasolQuote::quote($this->schemaName),
            ExasolQuote::quote($this->tableName),
            ExasolQuote::quote('PRIMARY KEY')
        );
        /** @var array<array{COLUMN_NAME:string}> $data */
        $data = $this->connection->fetchAllAssociative($sql);

        return array_map(static fn($column) => $column['COLUMN_NAME'], $data);
    }

    public function getTableStats(): TableStatsInterface
    {
        $sql = sprintf(
            '
            SELECT "RAW_OBJECT_SIZE"
  FROM "SYS"."EXA_ALL_OBJECT_SIZES"
  WHERE "OBJECT_NAME" = %s AND "ROOT_NAME" = %s AND "ROOT_TYPE" = %s
            ',
            ExasolQuote::quote($this->tableName),
            ExasolQuote::quote($this->schemaName),
            ExasolQuote::quote('SCHEMA')
        );
        /** @var int|string $result */
        $result = $this->connection->fetchOne($sql);

        return new TableStats((int) $result, $this->getRowsCount());
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
        $sql = sprintf(
            '
SELECT 
    "OBJECT_SCHEMA" AS "schema_name", 
    "OBJECT_NAME" AS "name" 
FROM "SYS"."EXA_ALL_DEPENDENCIES"  
WHERE "REFERENCED_OBJECT_SCHEMA" = %s AND "REFERENCED_OBJECT_NAME" = %s',
            ExasolQuote::quote($this->schemaName),
            ExasolQuote::quote($this->tableName)
        );

        return $this->connection->fetchAllAssociative($sql);
    }


    public function getTableDefinition(): TableDefinitionInterface
    {
        return new ExasolTableDefinition(
            $this->schemaName,
            $this->tableName,
            $this->isTemporary(),
            $this->getColumnsDefinitions(),
            $this->getPrimaryKeysNames()
        );
    }
}
