<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Snowflake;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Snowflake\SnowflakeImportAdapter;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;

    public function testGetCopyCommands(): void
    {
        /** @var Storage\Snowflake\Table|MockObject $source */
        $source = self::createMock(Storage\Snowflake\Table::class);
        $source->expects(self::once())->method('getSchema')->willReturn('schema');
        $source->expects(self::once())->method('getTableName')->willReturn('table');

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions([], ['col1', 'col2']);
        $adapter = new SnowflakeImportAdapter($this->mockConnection());
        $commands = $adapter->getCopyCommands(
            $source,
            $destination,
            $options,
            'stagingTable'
        );

        self::assertSame([
            'INSERT INTO "schema"."stagingTable" ("col1", "col2") SELECT "col1", "col2" FROM "schema"."table"',
        ], $commands);
    }
}
