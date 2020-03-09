<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\Synapse;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Synapse\Importer as SynapseImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class TableTest extends TestCase
{
    public function testDefaultValues(): void
    {
        $source = new Storage\Synapse\Table('schema', 'table');
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('schema', $source->getSchema());
        self::assertEquals('table', $source->getTableName());
        self::assertEquals([], $source->getQueryBindings());
        self::assertEquals('[schema].[table]', $source->getFromStatement());
    }

//  TODO: Export adapter

    public function testGetBackendImportAdapter(): void
    {
        $source = new Storage\Synapse\Table('schema', 'table');
        /** @var SynapseImporter|MockObject $importer */
        $importer = self::createMock(SynapseImporter::class);
        $adapter = $source->getBackendImportAdapter($importer);
        self::assertInstanceOf(BackendImportAdapterInterface::class, $adapter);
        self::assertInstanceOf(Storage\Synapse\SynapseImportAdapter::class, $adapter);
    }

    public function testGetBackendImportAdapterInvalidImporter(): void
    {
        $source = new Storage\Synapse\Table('schema', 'table');
        $dummyImporter = new class implements ImporterInterface {
            public function importTable(
                Storage\SourceInterface $source,
                Storage\DestinationInterface $destination,
                ImportOptions $options
            ): Result {
                return new Result([]);
            }
        };

        self::expectException(NoBackendAdapterException::class);
        $source->getBackendImportAdapter($dummyImporter);
    }
}
