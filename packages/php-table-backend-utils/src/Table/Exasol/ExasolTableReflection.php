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
        // TODO detect temp tables
        $this->isTemporary = false;

        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                'DESCRIBE %s.%s',
                ExasolQuote::quoteSingleIdentifier($this->schemaName),
                ExasolQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        return DataHelper::extractByKey($columns, 'COLUMN_NAME');
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        $columns = $this->connection->fetchAllAssociative(
            sprintf('
                SELECT "COLUMN_NAME", "COLUMN_IS_NULLABLE", "COLUMN_MAXSIZE", "COLUMN_NUM_PREC", "COLUMN_NUM_SCALE", "COLUMN_DEFAULT", "COLUMN_TYPE"
  FROM "SYS"."EXA_ALL_COLUMNS"
  WHERE "COLUMN_SCHEMA" = %s
    AND "COLUMN_TABLE" = %s
  ORDER BY "COLUMN_ORDINAL_POSITION"',
                ExasolQuote::quote($this->schemaName),
                ExasolQuote::quote($this->tableName)
            )
        );

        $columns = array_map(static function ($col) {
            $defaultValue = $col["COLUMN_DEFAULT"];
            return new ExasolColumn(
                $col['COLUMN_NAME'],
                new Exasol(
                    self::extractColumnType($col['COLUMN_TYPE']),
                    [
                        'length' => self::extractColumnLength($col['COLUMN_MAXSIZE'], $col['COLUMN_NUM_PREC'], $col['COLUMN_NUM_SCALE']),
                        'nullable' => $col['COLUMN_IS_NULLABLE'] === '1',
                        'default' => is_string($defaultValue) ? trim($defaultValue) : $defaultValue,
                    ]
                )
            );
        }, $columns);

        return new ColumnCollection($columns);
    }

    private function extractColumnType($type): string
    {
        if (!preg_match('/(?P<type>[a-zA-Z ]+)\(?.*/', $type, $output_array)) {
            throw new \Exception("Unknown type {$type}");
        } else {
            return $output_array['type'];
        }
    }

    private function extractColumnLength($maxLength, $precision = null, $scale = null): string
    {
        return ($precision === null && $scale === null) ? $maxLength : "{$precision},{$scale}";
    }

    public function getRowsCount(): int
    {
        $result = $this->connection->fetchOne(sprintf(
            'SELECT COUNT(*) AS NumberOfRows FROM %s.%s',
            ExasolQuote::quoteSingleIdentifier($this->schemaName),
            ExasolQuote::quoteSingleIdentifier($this->tableName)
        ));
        return (int) $result;
    }

    /**
     * returns list of column names where PK is defined on
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
        $data = $this->connection->fetchAllAssociative($sql);
        return DataHelper::extractByKey($data, 'COLUMN_NAME');
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
        $result = $this->connection->fetchOne($sql);

        return new TableStats((int) $result, $this->getRowsCount());
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }

    /**
     * @return array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public function getDependentViews(): array
    {
//        TODO
        throw new \Exception('method is not implemented yet');
    }


    public function getTableDefinition(): TableDefinitionInterface
    {
        // TODO
    }
}
