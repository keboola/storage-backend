<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\FromTableInsertIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockDbalConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromTableInsertIntoAdapterTest extends BaseTestCase
{
    use MockDbalConnectionTrait;

    public function testGetCopyCommands(): void
    {
        $source = new Storage\Snowflake\Table('test_schema', 'test_table', ['col1', 'col2']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
        // phpcs:ignore
            'INSERT INTO "test_schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "test_schema"."test_table"'
        );
        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 10,
                    'name' => 'stagingTable',
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 10,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $options = new SnowflakeImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsSelectSource(): void
    {
        $source = new Storage\Snowflake\SelectSource(
            'SELECT * FROM "test_schema"."test_table"',
            ['bind' => 'val'],
            ['col1', 'col2'],
            [],
            ['1'],
        );

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeQuery')->with(
        // phpcs:ignore
            'INSERT INTO "test_schema"."stagingTable" ("col1", "col2") SELECT * FROM "test_schema"."test_table"',
            ['bind' => 'val'],
            [1],
        );
        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 10,
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 10,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SnowflakeColumn::createGenericColumn('col1'),
                SnowflakeColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $options = new SnowflakeImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }
}
