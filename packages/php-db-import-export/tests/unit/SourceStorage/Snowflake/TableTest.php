<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Snowflake;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter as SnowflakeExporter;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new Storage\Snowflake\Table('schema', 'table');
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('schema', $source->getSchema());
        self::assertEquals('table', $source->getTableName());
        self::assertEquals([], $source->getQueryBindings());
        self::assertEquals('"schema"."table"', $source->getFromStatement());
    }
}
