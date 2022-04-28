<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Storage\S3;

use Keboola\Db\Import\Exception;
use Tests\Keboola\Db\ImportExportCommon\S3SourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use S3SourceTrait;

    public function testGetManifestEntries(): void
    {
        $source = $this->createS3SourceInstance('file.csv');
        $entries = $source->getManifestEntries();
        self::assertCount(1, $entries);
    }

    public function testGetManifestEntriesIncremental(): void
    {
        $source = $this->createS3SourceInstance('sliced/accounts/S3.accounts.csvmanifest', [], true);
        $entries = $source->getManifestEntries();
        self::assertCount(2, $entries);
    }

    public function testGetManifestNotExist(): void
    {
        $source = $this->createS3SourceInstance('02_tw_accounts.csv.notfound.manifest', [], true);

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        $source->getManifestEntries();
    }
}
