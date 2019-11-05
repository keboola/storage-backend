<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\SourceStorage\Snowflake;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage\NoBackendAdapterException;
use Keboola\Db\ImportExport\SourceStorage;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class SourceTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new SourceStorage\Snowflake\Source('schema', 'table');
        self::assertInstanceOf(SourceStorage\SourceInterface::class, $source);
        self::assertEquals('schema', $source->getSchema());
        self::assertEquals('table', $source->getTableName());
    }

    public function testGetBackendImportAdapter(): void
    {
        $source = new SourceStorage\Snowflake\Source('schema', 'table');
        /** @var SnowflakeImporter|MockObject $importer */
        $importer = self::createMock(SnowflakeImporter::class);
        $adapter = $source->getBackendImportAdapter($importer);
        self::assertInstanceOf(BackendImportAdapterInterface::class, $adapter);
        self::assertInstanceOf(SnowflakeImportAdapterInterface::class, $adapter);
        self::assertInstanceOf(SourceStorage\Snowflake\SnowflakeAdapter::class, $adapter);
    }

    public function testGetBackendImportAdapterInvalidImporter(): void
    {
        $source = new SourceStorage\Snowflake\Source('schema', 'table');
        $dummyImporter = new class implements ImporterInterface
        {
            public function importTable(
                ImportOptions $options,
                SourceStorage\SourceInterface $source
            ): Result {
                return new Result([]);
            }
        };

        self::expectException(NoBackendAdapterException::class);
        $source->getBackendImportAdapter($dummyImporter);
    }
}
