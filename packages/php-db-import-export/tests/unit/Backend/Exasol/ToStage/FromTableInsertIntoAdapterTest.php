<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Exasol\ToStage;

use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\FromTableInsertIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\Backend\Exasol\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromTableInsertIntoAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;

    public function testGetCopyCommands(): void
    {
        $source = new Storage\Exasol\Table('test_schema', 'test_table', ['col1', 'col2']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
        // phpcs:ignore
            'INSERT INTO "test_schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "test_schema"."test_table"'
        );
        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "test_schema"."stagingTable"')
            ->willReturn(10);

        $destination = new ExasolTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $options = new ExasolImportOptions([]);
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
        $source = new Storage\Exasol\SelectSource(
            'SELECT * FROM "test_schema"."test_table"',
            ['val'],
            ['1'],
            ['col1', 'col2'],
        );

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeQuery')->with(
        // phpcs:ignore
            'INSERT INTO "test_schema"."stagingTable" ("col1", "col2") SELECT * FROM "test_schema"."test_table"',
            ['val'],
            [1],
        );
        $conn->expects(self::once())->method('fetchOne')
            ->with('SELECT COUNT(*) AS NumberOfRows FROM "test_schema"."stagingTable"')
            ->willReturn(10);

        $destination = new ExasolTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                ExasolColumn::createGenericColumn('col1'),
                ExasolColumn::createGenericColumn('col2'),
            ]),
            [],
        );
        $options = new ExasolImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }
}
