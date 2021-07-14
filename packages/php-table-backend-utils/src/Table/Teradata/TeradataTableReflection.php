<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table\Teradata;

use Doctrine\DBAL\Connection;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Collection;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
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

        return Collection::extractByKey($columns, 'Column Name');
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

        $columns = array_map(static function ($col) {
            $colName = trim($col['Column Name']);
            $colType = trim($col['Type']);
            $defaultvalue = $col['Default value'];
            return new TeradataColumn(
                $colName,
                new Teradata(
                    Teradata::convertCodeToType($colType),
                    [
                        'length' => $col['Max Length'],
                        'nullable' => $col['Nullable'] === 'Y',
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
         AND TableName = %s;",
            TeradataQuote::quote($this->dbName),
            TeradataQuote::quote($this->tableName)
        );

        $data = $this->connection->fetchAllAssociative($sql);
        return Collection::extractByKey($data, 'ColumnName');
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
