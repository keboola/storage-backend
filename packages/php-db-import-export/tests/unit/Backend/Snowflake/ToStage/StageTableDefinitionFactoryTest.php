<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\StageTableDefinitionFactory;
use Keboola\TableBackendUtils\Column\ColumnCollection;

use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class StageTableDefinitionFactoryTest extends BaseTestCase
{
    public function testCreateStagingTableDefinitionWithTypes(): void
    {
        $definition = new SnowflakeTableDefinition(
            'schema',
            'table',
            false,
            new ColumnCollection([
                new SnowflakeColumn('name', new Snowflake(Snowflake::TYPE_DATE)),
                SnowflakeColumn::createGenericColumn('id'),
            ]),
            []
        );
        $stageDefinition = StageTableDefinitionFactory::createStagingTableDefinition(
            $definition,
            ['id', 'name', 'notInDef']
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('__temp_', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame(['id', 'name', 'notInDef'], $stageDefinition->getColumnsNames());
        /** @var SnowflakeColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        // id is NVARCHAR
        self::assertSame(Snowflake::TYPE_VARCHAR, $definitions[0]->getColumnDefinition()->getType());
        // name is DATE
        self::assertSame(Snowflake::TYPE_DATE, $definitions[1]->getColumnDefinition()->getType());
        // notInDef has default NVARCHAR
        self::assertSame(
            Snowflake::TYPE_VARCHAR,
            $definitions[2]->getColumnDefinition()->getType()
        );
    }

    public function testCreateStagingTableDefinitionWithText(): void
    {
        $columns = ['id', 'name', 'number', 'notInDef'];
        $stageDefinition = StageTableDefinitionFactory::createVarcharStagingTableDefinition(
            'schema',
            $columns
        );

        self::assertSame('schema', $stageDefinition->getSchemaName());
        self::assertStringStartsWith('__temp_', $stageDefinition->getTableName());
        self::assertTrue($stageDefinition->isTemporary());
        // order same as source
        self::assertSame($columns, $stageDefinition->getColumnsNames());
        /** @var SnowflakeColumn[] $definitions */
        $definitions = iterator_to_array($stageDefinition->getColumnsDefinitions());
        foreach ($columns as $i => $columnName) {
            self::assertSame(Snowflake::TYPE_VARCHAR, $definitions[$i]->getColumnDefinition()->getType());
            self::assertSame($columnName, $definitions[$i]->getColumnName());
        }
    }
}
