<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\SourceStorage\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage\Snowflake\SnowflakeAdapter;
use Keboola\Db\ImportExport\SourceStorage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeAdapterTest extends BaseTestCase
{
    public function testExecuteCopyCommands(): void
    {
        /** @var SourceStorage\Snowflake\Source|MockObject $source */
        $source = self::createMock(SourceStorage\Snowflake\Source::class);
        /** @var Connection|MockObject $connection */
        $connection = self::createMock(Connection::class);
        $connection->expects(self::once())->method('fetchAll')->willReturn([['count' => 1]]);
        /** @var ImportState|MockObject $state */
        $state = self::createMock(ImportState::class);
        $state->expects(self::once())->method('startTimer');
        $state->expects(self::once())->method('stopTimer');
        /** @var ImportOptions|MockObject $options */
        $options = self::createMock(ImportOptions::class);

        $adapter = new SnowflakeAdapter($source);
        $rows = $adapter->executeCopyCommands(
            ['cmd1'],
            $connection,
            $options,
            $state
        );

        self::assertEquals(1, $rows);
    }

    public function testGetCopyCommands(): void
    {
        /** @var SourceStorage\Snowflake\Source|MockObject $source */
        $source = self::createMock(SourceStorage\Snowflake\Source::class);
        $source->expects(self::once())->method('getSchema')->willReturn('schema');
        $source->expects(self::once())->method('getTableName')->willReturn('table');

        $options = new ImportOptions('schema', 'table', [], ['col1', 'col2']);
        $adapter = new SnowflakeAdapter($source);
        $commands = $adapter->getCopyCommands(
            $options,
            'stagingTable'
        );

        self::assertSame([
            'INSERT INTO "schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "schema"."table"',
        ], $commands);
    }
}
