<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Snowflake;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Snowflake\SnowflakeImportAdapter;
use Tests\Keboola\Db\ImportExportCommon\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;
    use ABSSourceTrait;

    public function testIsSupported(): void
    {
        $absSource = $this->createDummyABSSourceInstance('');
        $snowflakeTable = new Storage\Snowflake\Table('', '');
        $snowflakeSelectSource = new Storage\Snowflake\SelectSource('', []);
        $synapseTable = new Storage\Synapse\Table('', '');

        $this->assertTrue(
            SnowflakeImportAdapter::isSupported(
                $snowflakeTable,
                $snowflakeTable
            )
        );

        $this->assertFalse(
            SnowflakeImportAdapter::isSupported(
                $snowflakeSelectSource,
                $synapseTable
            )
        );

        $this->assertFalse(
            SnowflakeImportAdapter::isSupported(
                $absSource,
                $synapseTable
            )
        );
    }

    public function testGetCopyCommands(): void
    {
        $source = new Storage\Snowflake\Table('schema', 'table', ['col1', 'col2']);

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('query')->with(
            'INSERT INTO "schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "schema"."table"'
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS "count" FROM "schema"."stagingTable"')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Snowflake\Table('schema', 'table', ['col1', 'col2']);
        $options = new ImportOptions([]);
        $adapter = new SnowflakeImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
        );

        $this->assertEquals(10, $count);
    }
}
