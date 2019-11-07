<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter as SnowflakeExporter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class DestinationFileTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testDefaultValues(): void
    {
        $source = $this->createABSSourceDestinationInstance('file.csv');
        self::assertInstanceOf(Storage\ABS\BaseFile::class, $source);
        self::assertInstanceOf(Storage\DestinationInterface::class, $source);
    }

    public function testGetBackendExportAdapter(): void
    {
        $source = $this->createABSSourceDestinationInstance('file.csv');
        /** @var SnowflakeExporter|MockObject $importer */
        $importer = self::createMock(SnowflakeExporter::class);
        $adapter = $source->getBackendExportAdapter($importer);
        self::assertInstanceOf(BackendExportAdapterInterface::class, $adapter);
        self::assertInstanceOf(Storage\ABS\SnowflakeExportAdapter::class, $adapter);
    }

    public function testGetBackendExportAdapterInvalidExporter(): void
    {
        $source = $this->createABSSourceDestinationInstance('file.csv');
        $dummyExporter = new class implements ExporterInterface
        {
            public function exportTable(
                Storage\SourceInterface $source,
                Storage\DestinationInterface $destination,
                ExportOptions $options
            ): void {
            }
        };

        self::expectException(NoBackendAdapterException::class);
        $source->getBackendExportAdapter($dummyExporter);
    }
}
