<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3;

class ExportTest extends TeradataBaseTestCase
{
    private const EXPORT_DIR = 'teradata_test_export';

    public function setUp(): void
    {
        parent::setUp();

        $this->clearDestination($this->getExportDir());

        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    private function getExportDir(): string
    {
        $buildPrefix = '';
        if (getenv('BUILD_PREFIX') !== false) {
            $buildPrefix = getenv('BUILD_PREFIX');
        }

        return self::EXPORT_DIR
            . '-'
            . $buildPrefix
            . '-'
            . getenv('SUITE');
    }

    public function tearDown(): void
    {
        parent::tearDown();
        unset($this->client);
    }

    public function testExportGzip(): void
    {
        // TODO
    }


    /**
     * @param Storage\Teradata\Table $destination
     * @param S3\SourceFile|S3\SourceDirectory $source
     * @param TeradataImportOptions $options
     * @throws \Doctrine\DBAL\Exception
     */
    private function importTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options
    ): void {
        // TODO

    }

    public function testExportSimple(): void
    {
        // TODO

    }

    public function testExportSimpleWithQuery(): void
    {
        // TODO
    }
}
