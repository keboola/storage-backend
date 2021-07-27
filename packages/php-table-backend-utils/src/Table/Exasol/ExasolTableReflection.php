<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Exasol;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
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
                TeradataQuote::quoteSingleIdentifier($this->schemaName),
                TeradataQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        return DataHelper::extractByKey($columns, 'COLUMN_NAME');
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                'HELP TABLE %s.%s',
                TeradataQuote::quoteSingleIdentifier($this->schemaName),
                TeradataQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        // types with structure of length <totalDigits>,<fractionalDigits> hidden in extra columns in table description
        $fractionalTypes = [
            'D',
            'N',
            'SC',
            'DS',
            'HS',
            'MS',
            'DS',
        ];
        $totalTypes = [
            'MI',
            'YR',
            'MO',
            'DY',
            'HR',
            'HM',
            'DM',
            'YM',
            'DH',
        ];

        // types with length described in fractionalDigits column
        $timeTypes = [
            'AT',
            'TS',
            'TZ',
            'SZ',
            'PT',
            'PZ',
            'PM',
        ];

        $charTypes = ['CF', 'CV', 'CO'];
        $columns = array_map(static function ($col) use ($fractionalTypes, $timeTypes, $charTypes, $totalTypes) {
            $colName = trim($col['Column Name']);
            $colType = trim($col['Type']);
            $defaultvalue = $col['Default value'];
            $length = $col['Max Length'];
            $isLatin = $col['Char Type'] === '1';
            // 1 latin, 2 unicode, 3 kanjiSJIS, 4 graphic, 5 graphic, 0 others

            if (in_array($colType, $fractionalTypes, true)) {
                $length = "{$col['Decimal Total Digits']},{$col['Decimal Fractional Digits']}";
            }
            if (in_array($colType, $timeTypes, true)) {
                $length = $col['Decimal Fractional Digits'];
            }
            if (in_array($colType, $totalTypes, true)) {
                $length = $col['Decimal Total Digits'];
            }

            if (!$isLatin && in_array($colType, $charTypes, true)) {
                $length /= 2;
                // non latin chars (unicode etc) declares double of space for data
            }
            return new TeradataColumn(
                $colName,
                new Teradata(
                    Teradata::convertCodeToType($colType),
                    [
                        'length' => $length,
                        'nullable' => $col['Nullable'] === 'Y',
                        'isLatin' => $isLatin,
                        'default' => is_string($defaultvalue) ? trim($defaultvalue) : $defaultvalue,
                    ]
                )
            );
        }, $columns);

        return new ColumnCollection($columns);
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
