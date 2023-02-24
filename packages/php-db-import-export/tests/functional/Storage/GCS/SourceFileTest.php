<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Storage\GCS;

use Tests\Keboola\Db\ImportExportCommon\GCSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use GCSSourceTrait;

    protected function getGCSBucketEnvName(): string
    {
        return 'GCS_BUCKET_NAME';
    }

    public function testGetManifestEntries(): void
    {
        $source = $this->createGCSSourceInstance('file.csv');
        $entries = $source->getManifestEntries();
        self::assertCount(1, $entries);
    }

    public function testGetManifestEntriesIncremental(): void
    {
        $source = $this->createGCSSourceInstance('sliced/accounts/GCS.accounts.csvmanifest', [], true);
        $entries = $source->getManifestEntries();
        self::assertCount(2, $entries);
    }
}
