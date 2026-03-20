<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter as SnowflakeExporter;
use PHPUnit\Framework\TestCase;

/**
 * exporter is covered by functional tests
 */
class ExporterTest extends TestCase
{
    public function test(): void
    {
        $connection = self::createStub(Connection::class);
        $exporter = new SnowflakeExporter($connection);
        self::assertInstanceOf(ExporterInterface::class, $exporter); // @phpstan-ignore staticMethod.alreadyNarrowedType
    }
}
