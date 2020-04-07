<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Table;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\ReflectionException;

final class SynapseTableReflection implements TableReflectionInterface
{
    /** @var Connection */
    private $connection;

    /** @var string */
    private $schemaName;

    /** @var string */
    private $tableName;

    /** @var string */
    private $objectId;

    /** @var bool */
    private $isTemporary;

    /** @var SQLServer2012Platform|AbstractPlatform */
    private $platform;

    public function __construct(Connection $connection, string $schemaName, string $tableName)
    {
        // temporary tables starts with #
        $this->isTemporary = strpos($tableName, '#') === 0;
        $this->tableName = $tableName;
        $this->schemaName = $schemaName;
        $this->connection = $connection;
        $this->platform = $connection->getDatabasePlatform();
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        if ($this->isTemporary) {
            $this->throwTemporaryTableException();
        }
        $tableId = $this->getObjectId();

        $columns = $this->connection->fetchAll(sprintf(
            'SELECT [NAME] FROM sys.columns WHERE object_id = %s ',
            $this->connection->quote($tableId)
        ));

        return array_map(static function ($column) {
            return $column['NAME'];
        }, $columns);
    }

    /**
     * tempdb is not implemented in synapse and information about tables cannot be obtained
     */
    private function throwTemporaryTableException(): void
    {
        throw new ReflectionException('Temporary tables cannot be reflected in Synapse.');
    }

    public function getObjectId(): string
    {
        if ($this->objectId !== null) {
            return $this->objectId;
        }

        if ($this->isTemporary) {
            $object = $this->connection->quote('tempdb..' . $this->tableName);
        } else {
            $object = $this->connection->quote($this->schemaName . '.' . $this->tableName);
        }

        /** @var string|null $objectId */
        $objectId = $this->connection->fetchColumn(sprintf(
            'SELECT OBJECT_ID(N%s)',
            $object
        ));

        if ($objectId === null) {
            throw new ReflectionException(sprintf(
                'Table %s.%s does not exist.',
                $this->schemaName,
                $this->tableName
            ));
        }

        $this->objectId = $objectId;
        return $this->objectId;
    }

    public function getColumnsDefinitions(): ColumnCollection
    {
        if ($this->isTemporary) {
            $this->throwTemporaryTableException();
        }

        $tableId = $this->getObjectId();

        $sql = <<<EOT
SELECT 
    c.name AS column_name,
    c.precision AS column_precision,
    c.scale AS column_scale,
    c.max_length AS column_length,
    c.is_nullable AS column_is_nullable,
    d.definition AS column_default,
    t.name AS column_type
FROM sys.columns AS c
JOIN sys.types AS t ON t.user_type_id = c.user_type_id
LEFT JOIN sys.default_constraints AS d ON c.default_object_id = d.object_id
WHERE c.object_id = '$tableId';
EOT;

        /** @var array{
         *     column_name:string,
         *     column_type:string,
         *     column_length:string,
         *     column_precision:string,
         *     column_scale:string,
         *     column_is_nullable:string,
         *     column_default:?string
         * }[] $columns */
        $columns = $this->connection->fetchAll($sql);

        $columns = array_map(static function ($col) {
            return SynapseColumn::createFromDB($col);
        }, $columns);

        return new ColumnCollection($columns);
    }

    public function getRowsCount(): int
    {
        $count = $this->connection->fetchColumn(sprintf(
            'SELECT COUNT(*) AS [count] FROM %s.%s',
            $this->platform->quoteSingleIdentifier($this->schemaName),
            $this->platform->quoteSingleIdentifier($this->tableName)
        ));

        return (int) $count;
    }

    /**
     * @return string[]
     */
    public function getPrimaryKeysNames(): array
    {
        if ($this->isTemporary) {
            $this->throwTemporaryTableException();
        }

        $tableId = $this->getObjectId();

        $result = $this->connection->fetchAll(
            <<< EOT
SELECT COL_NAME(ic.OBJECT_ID,ic.column_id) AS column_name
    FROM sys.indexes AS i INNER JOIN
        sys.index_columns AS ic ON i.OBJECT_ID = ic.OBJECT_ID AND i.index_id = ic.index_id
    WHERE i.is_primary_key = 1 AND i.OBJECT_ID = '$tableId'
EOT
        );

        return array_map(static function ($item) {
            return $item['column_name'];
        }, $result);
    }

    public function getTableStats(): TableStatsInterface
    {
        if ($this->isTemporary) {
            $this->throwTemporaryTableException();
        }

        /**
         * @var array{
         *  name: string,
         *  rows: string,
         *  reserved: string,
         *  data: string,
         *  index_size: string,
         *  unused: string,
         * } $info
         */
        $info = $this->connection->fetchAssoc(sprintf(
            'EXEC sp_spaceused \'%s.%s\'',
            $this->platform->quoteSingleIdentifier($this->schemaName),
            $this->platform->quoteSingleIdentifier($this->tableName)
        ));

        return new TableStats((int) $info['data'], (int) $info['rows']);
    }

    public function isTemporary(): bool
    {
        return $this->isTemporary;
    }
}
