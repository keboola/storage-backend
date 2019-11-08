<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\SourceStorage\ABS;

use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportAdapterInterface;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;
use Keboola\Db\ImportExport\SourceStorage\ABS\SnowflakeAdapter;
use Keboola\Db\ImportExport\SourceStorage\NoBackendAdapterException;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testDefaultValues(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
        self::assertInstanceOf(SourceStorage\SourceInterface::class, $source);
        self::assertEquals('file.csv', $source->getCsvFile()->getFilename());
        self::assertEquals('azure://absAccount.blob.core.windows.net/absContainer/', $source->getContainerUrl());
        self::assertEquals('azureCredentials', $source->getSasToken());
    }

    public function testGetBackendImportAdapter(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
        /** @var SnowflakeImporter|MockObject $importer */
        $importer = self::createMock(SnowflakeImporter::class);
        $adapter = $source->getBackendImportAdapter($importer);
        self::assertInstanceOf(BackendImportAdapterInterface::class, $adapter);
        self::assertInstanceOf(SnowflakeImportAdapterInterface::class, $adapter);
        self::assertInstanceOf(SnowflakeAdapter::class, $adapter);
    }

    public function testGetBackendImportAdapterInvalidImporter(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
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

    public function testGetManifestEntries(): void
    {
        $source = $this->createDummyABSSourceInstance('empty.csv');
        self::assertSame(
            ['azure://absAccount.blob.core.windows.net/absContainer/empty.csv'],
            $source->getManifestEntries()
        );
    }

    public function testGetManifestEntriesIncremental(): void
    {
        $source = $this->createABSSourceInstance('sliced/accounts/accounts.csvmanifest', true);
        $entries = $source->getManifestEntries();
        self::assertCount(2, $entries);
    }
}
