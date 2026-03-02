<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Info;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Info\TableInfo\TableColumn;
use Keboola\StorageDriver\Command\Info\TableType as DriverTableType;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\Table\TableType;

class TableReflectionResponseTransformer
{
    public static function transformTableReflectionToResponse(
        string $schemaName,
        SnowflakeTableReflection $ref,
    ): TableInfo {
        $res = new TableInfo();
        $def = $ref->getTableDefinition();

        $columns = new RepeatedField(GPBType::MESSAGE, TableColumn::class);
        /** @var SnowflakeColumn $col */
        foreach ($def->getColumnsDefinitions() as $col) {
            /** @var Snowflake $colDef */
            $colDef = $col->getColumnDefinition();

            $colInternal = (new TableColumn())
                ->setName($col->getColumnName())
                ->setType($colDef->getType())
                ->setNullable($colDef->isNullable());

            if ($colDef->getLength() !== null) {
                $colInternal->setLength($colDef->getLength());
            }

            if ($colDef->getDefault() !== null) {
                $colInternal->setDefault($colDef->getDefault());
            }

            $columns[] = $colInternal;
        }
        $res->setColumns($columns);

        $path = new RepeatedField(GPBType::STRING);
        $path[] = $schemaName;
        $res->setPath($path);

        $res->setTableName($def->getTableName());
        $pk = new RepeatedField(GPBType::STRING);

        foreach ($def->getPrimaryKeysNames() as $col) {
            $pk[] = $col;
        }
        $res->setPrimaryKeysNames($pk);

        $stats = $ref->getTableStats();
        $res->setRowsCount($stats->getRowsCount());
        $res->setSizeBytes($stats->getDataSizeBytes());

        $res->setTableType(
            match ($ref->getTableDefinition()->getTableType()) {
                TableType::SNOWFLAKE_EXTERNAL => DriverTableType::EXTERNAL,
                default => DriverTableType::NORMAL,
            },
        );

        return $res;
    }
}
