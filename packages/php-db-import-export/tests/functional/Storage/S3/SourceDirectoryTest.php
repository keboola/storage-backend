<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Storage\S3;

use Keboola\CsvOptions\CsvOptions;
use Tests\Keboola\Db\ImportExportCommon\S3SourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceDirectoryTest extends BaseTestCase
{
    use S3SourceTrait;

    public function testGetManifestEntriesFolder(): void
    {
        $source = $this->createS3SourceInstanceFromCsv(
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
        $source = $this->createS3SourceInstanceFromCsv(
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
