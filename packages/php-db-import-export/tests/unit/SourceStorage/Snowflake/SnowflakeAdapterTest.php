<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Snowflake\SnowflakeImportAdapter;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeAdapterTest extends BaseTestCase
{
    public function testExecuteCopyCommands(): void
    {
        /** @var Connection|MockObject $connection */
        $connection = self::createMock(Connection::class);
        $connection->expects(self::once())->method('fetchAll')->willReturn([['count' => 1]]);
        /** @var ImportState|MockObject $state */
        $state = self::createMock(ImportState::class);
        $state->expects(self::once())->method('startTimer');
        $state->expects(self::once())->method('stopTimer');
        /** @var ImportOptions|MockObject $options */
        $options = self::createMock(ImportOptions::class);

        $adapter = new SnowflakeImportAdapter(new Storage\Snowflake\Table('schema', 'table'));
        $rows = $adapter->executeCopyCommands(
            ['cmd1'],
            $connection,
            new Storage\Snowflake\Table('schema', 'table'),
            $options,
            $state
        );

        self::assertEquals(1, $rows);
    }

    public function testGetCopyCommands(): void
    {
        /** @var Storage\Snowflake\Table|MockObject $source */
        $source = self::createMock(Storage\Snowflake\Table::class);
        $source->expects(self::once())->method('getSchema')->willReturn('schema');
        $source->expects(self::once())->method('getTableName')->willReturn('table');

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions([], ['col1', 'col2']);
        $adapter = new SnowflakeImportAdapter($source);
        $commands = $adapter->getCopyCommands(
            $destination,
            $options,
            'stagingTable'
        );

        self::assertSame([
            'INSERT INTO "schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "schema"."table"',
        ], $commands);
    }
}
