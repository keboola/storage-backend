<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Synapse;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Synapse\SynapseImportAdapter;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\Backend\Synapse\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SynapseAdapterTest extends BaseTestCase
{
    use ABSSourceTrait;
    use MockConnectionTrait;

    public function testIsSupported(): void
    {
        $absSource = $this->createDummyABSSourceInstance('');
        $snowflakeTable = new Storage\Snowflake\Table('', '');
        $snowflakeSelectSource = new Storage\Snowflake\SelectSource('', []);
        $synapseTable = new Storage\Synapse\Table('', '');
        $synapseSelectSource = new Storage\Synapse\SelectSource('', []);

        $this->assertTrue(
            SynapseImportAdapter::isSupported(
                $synapseTable,
                $synapseTable
            )
        );

        $this->assertTrue(
            SynapseImportAdapter::isSupported(
                $synapseSelectSource,
                $synapseTable
            )
        );

        $this->assertFalse(
            SynapseImportAdapter::isSupported(
                $snowflakeSelectSource,
                $synapseTable
            )
        );

        $this->assertFalse(
            SynapseImportAdapter::isSupported(
                $snowflakeSelectSource,
                $snowflakeTable
            )
        );

        $this->assertFalse(
            SynapseImportAdapter::isSupported(
                $absSource,
                $snowflakeTable
            )
        );

        $this->assertFalse(
            SynapseImportAdapter::isSupported(
                $snowflakeTable,
                $synapseTable
            )
        );
    }

    public function testGetCopyCommands(): void
    {
        /** @var Storage\Synapse\Table|MockObject $source */
        $source = self::createMock(Storage\Synapse\Table::class);
        $source->expects(self::once())->method('getSchema')->willReturn('schema');
        $source->expects(self::once())->method('getTableName')->willReturn('table');

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('exec')->with(
            'INSERT INTO [schema].[stagingTable] ([col1], [col2]) SELECT [col1], [col2] FROM [schema].[table]'
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions([], ['col1', 'col2']);
        $adapter = new SynapseImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
        );

        $this->assertEquals(10, $count);
    }

    public function testGetCopyCommandsSelectSource(): void
    {
        /** @var Storage\Synapse\SelectSource|MockObject $source */
        $source = self::createMock(Storage\Synapse\SelectSource::class);
        $source->expects(self::once())->method('getFromStatement')->willReturn('SELECT * FROM table.schema');
        $source->expects(self::once())->method('getQueryBindings')->willReturn(['bind1'=>1]);
        $source->expects(self::once())->method('getDataTypes')->willReturn(['bind1'=>1]);

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeQuery')->with(
            'INSERT INTO [schema].[stagingTable] ([col1], [col2]) SELECT * FROM table.schema'
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS [count] FROM [schema].[stagingTable]')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Synapse\Table('schema', 'table');
        $options = new ImportOptions([], ['col1', 'col2']);
        $adapter = new SynapseImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
        );

        $this->assertEquals(10, $count);
    }
}
