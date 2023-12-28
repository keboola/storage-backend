<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\ToStage;

use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\FromTableInsertIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromTableInsertIntoAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;

    public function testGetCopyCommands(): void
    {
        $source = new Storage\Synapse\Table('test_schema', 'test_table', ['col1', 'col2']);

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeStatement')->with(
        // phpcs:ignore
            'INSERT INTO [test_schema].[stagingTable] ([col1], [col2]) SELECT [col1], [col2] FROM [test_schema].[test_table]'
        );
        $conn->expects($this->once())->method('fetchOne')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [test_schema].[stagingTable]')
            ->willReturn(10);

        $destination = new SynapseTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP),
        );
        $options = new SynapseImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        $this->assertEquals(10, $count);
    }

    public function testGetCopyCommandsSelectSource(): void
    {
        $source = new Storage\Synapse\SelectSource(
            'SELECT * FROM [test_schema].[test_table]',
            ['val'],
            [1],
            ['col1','col2'],
        );

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeQuery')->with(
        // phpcs:ignore
            'INSERT INTO [test_schema].[stagingTable] ([col1], [col2]) SELECT * FROM [test_schema].[test_table]',
            ['val'],
            [1],
        );
        $conn->expects($this->once())->method('fetchOne')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [test_schema].[stagingTable]')
            ->willReturn(10);

        $destination = new SynapseTableDefinition(
            'test_schema',
            'stagingTable',
            true,
            new ColumnCollection([
                SynapseColumn::createGenericColumn('col1'),
                SynapseColumn::createGenericColumn('col2'),
            ]),
            [],
            new TableDistributionDefinition(TableDistributionDefinition::TABLE_DISTRIBUTION_ROUND_ROBIN),
            new TableIndexDefinition(TableIndexDefinition::TABLE_INDEX_TYPE_HEAP),
        );
        $options = new SynapseImportOptions([]);
        $adapter = new FromTableInsertIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        $this->assertEquals(10, $count);
    }
}
