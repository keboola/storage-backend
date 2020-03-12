<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\Db\Import\Exception;
use Keboola\Db\Import\Result;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer as SnowflakeImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS\SnowflakeImportAdapter;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testDefaultValues(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
        self::assertInstanceOf(Storage\ABS\BaseFile::class, $source);
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('file.csv', $source->getFilePath());
    }

    public function testGetBackendImportAdapter(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
        /** @var SnowflakeImporter|MockObject $importer */
        $importer = self::createMock(SnowflakeImporter::class);
        $adapter = $source->getBackendImportAdapter($importer);
        self::assertInstanceOf(BackendImportAdapterInterface::class, $adapter);
        self::assertInstanceOf(SnowflakeImportAdapter::class, $adapter);
    }

    public function testGetBackendImportAdapterInvalidImporter(): void
    {
        $source = $this->createDummyABSSourceInstance('file.csv');
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

    public function testGetManifestEntriesNotExistingCsv(): void
    {
        $source = $this->createABSSourceInstance('sliced/not_exists.csv');

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        $source->getManifestEntries();
    }

    public function testGetManifestEntriesNotExistingManifestEntry(): void
    {
        $source = $this->createABSSourceInstance('02_tw_accounts.csv.invalid.manifest', true);

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        $source->getManifestEntries();
    }
}
