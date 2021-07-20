<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\Table\TableReflectionInterface;
use Keboola\TableBackendUtils\Table\TableStats;
use Keboola\TableBackendUtils\Table\TableStatsInterface;

final class TeradataTableReflection implements TableReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $dbName;

    /** @var string */
    private $tableName;

    /** @var bool */
    private $isTemporary;

    public function __construct(Connection $connection, string $dbName, string $tableName)
    {
        // TODO detect temp tables
        $this->isTemporary = false;

        $this->tableName = $tableName;
        $this->dbName = $dbName;
        $this->connection = $connection;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                'HELP TABLE %s.%s',
                TeradataQuote::quoteSingleIdentifier($this->dbName),
                TeradataQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        return DataHelper::extractByKey($columns, 'Column Name');
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        $columns = $this->connection->fetchAllAssociative(
            sprintf(
                'HELP TABLE %s.%s',
                TeradataQuote::quoteSingleIdentifier($this->dbName),
                TeradataQuote::quoteSingleIdentifier($this->tableName)
            )
        );

        // types with structure of length <totalDigits>,<fractionalDigits> hidden in extra columns in table description
        $fractionalTypes = [
            'D',
            'N',
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
        $columns = array_map(static function ($col) use ($fractionalTypes, $timeTypes, $charTypes) {
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
            if (in_array($colType, $timeTypes, true)) {
                $length = $col['Decimal Fractional Digits'];
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
            TeradataQuote::quoteSingleIdentifier($this->dbName),
            TeradataQuote::quoteSingleIdentifier($this->tableName)
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
            "
        SELECT ColumnName FROM DBC.IndicesV WHERE
         IndexType = 'K'
         AND DatabaseName = %s 
         AND TableName = %s ORDER BY ColumnName;",
            TeradataQuote::quote($this->dbName),
            TeradataQuote::quote($this->tableName)
        );

        $data = $this->connection->fetchAllAssociative($sql);
        return DataHelper::extractByKey($data, 'ColumnName');
    }

    public function getTableStats(): TableStatsInterface
    {
        $sql = sprintf(
            '
SELECT CURRENTPERM FROM DBC.ALLSPACE
WHERE  DATABASENAME = %s AND TABLENAME = %s 
',
            TeradataQuote::quote($this->dbName),
            TeradataQuote::quote($this->tableName)
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
        return new TeradataTableDefinition(
            $this->dbName,
            $this->tableName,
            $this->isTemporary(),
            $this->getColumnsDefinitions(),
            $this->getPrimaryKeysNames()
        );
    }
}
