<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Snowflake;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter as SnowflakeExporter;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
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
        self::assertEquals([], $source->getColumnsNames());
        self::assertEquals('SELECT * FROM "schema"."table"', $source->getFromStatement());
        self::assertNull($source->getPrimaryKeysNames());
    }

    public function testColumns(): void
    {
        $source = new Storage\Snowflake\Table('schema', 'table', ['col1', 'col2']);
        self::assertEquals(['col1', 'col2'], $source->getColumnsNames());
        self::assertEquals('SELECT "col1", "col2" FROM "schema"."table"', $source->getFromStatement());
    }
}
