<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * importer is covered by functional tests
 */
class ImporterTest extends TestCase
{
    public function test(): void
    {
        /** @var Connection|MockObject $connection */
        $connection = self::createMock(Connection::class);
        $importer = new SnowflakeImporter($connection);
        self::assertInstanceOf(ImporterInterface::class, $importer);
    }
}
