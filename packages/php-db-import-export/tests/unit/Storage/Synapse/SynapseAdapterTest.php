<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Synapse;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\SQLServer2012Platform;
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

        $this->assertTrue(
            SynapseImportAdapter::isSupported(
                $synapseTable,
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
        $source = new Storage\Synapse\Table('test_schema', 'test_table', ['col1', 'col2']);

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('exec')->with(
        // phpcs:ignore
            'INSERT INTO [test_schema].[stagingTable] ([col1], [col2]) SELECT [col1], [col2] FROM [test_schema].[test_table]'
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [test_schema].[stagingTable]')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Synapse\Table('test_schema', 'test_table', ['col1', 'col2']);
        $options = new ImportOptions([]);
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
        $source = new Storage\Synapse\SelectSource(
            'SELECT * FROM [test_schema].[test_table]',
            ['val'],
            [1],
            ['col1','col2']
        );

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('executeQuery')->with(
        // phpcs:ignore
            'INSERT INTO [test_schema].[stagingTable] ([col1], [col2]) SELECT * FROM [test_schema].[test_table]',
            ['val'],
            [1]
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT_BIG(*) AS [count] FROM [test_schema].[stagingTable]')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Synapse\Table('test_schema', 'test_table', ['col1', 'col2']);
        $options = new ImportOptions([]);
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
