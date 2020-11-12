<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExport\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceDirectoryTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testGetManifestEntriesFolder(): void
    {
        $source = $this->createABSSourceInstanceFromCsv(
            'sliced_accounts_no_manifest',
            new CsvOptions(),
            [],
            true,
            true
        );
        $entries = $source->getManifestEntries();
        self::assertCount(2, $entries);
    }

    public function testGetManifestEntriesFolderWithTrailingSlash(): void
    {
        $source = $this->createABSSourceInstanceFromCsv(
            'sliced_accounts_no_manifest/',
            new CsvOptions(),
            [],
            true,
            true
        );
        $entries = $source->getManifestEntries();
        self::assertCount(2, $entries);
    }
}
