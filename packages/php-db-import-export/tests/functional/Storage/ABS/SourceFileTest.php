<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Storage\ABS;

use Keboola\Db\Import\Exception;
use Tests\Keboola\Db\ImportExportCommon\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use ABSSourceTrait;

    public function testGetManifestEntries(): void
    {
        $source = $this->createABSSourceInstance('file.csv');
        $entries = $source->getManifestEntries();
        self::assertCount(1, $entries);
    }

    public function testGetManifestEntriesIncremental(): void
    {
        $source = $this->createABSSourceInstance('sliced/accounts/ABS.accounts.csvmanifest', [], true);
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
        $source = $this->createABSSourceInstance('02_tw_accounts.csv.invalid.manifest', [], true);

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        $source->getManifestEntries();
    }

    public function testGetLineEndingSliced(): void
    {
        $source = $this->createABSSourceInstance('sliced/accounts/ABS.accounts.csvmanifest', [], true);
        self::assertSame('lf', $source->getLineEnding());
    }

    public function testGetLineEndingSingle(): void
    {
        $source = $this->createABSSourceInstance('file.csv');
        self::assertSame('lf', $source->getLineEnding());
    }

    public function testGetLineEndingSingleCrlf(): void
    {
        $source = $this->createABSSourceInstance('tw_accounts.crlf.csv');
        self::assertSame('crlf', $source->getLineEnding());
    }

    public function testGetLineEndingCompressed(): void
    {
        $source = $this->createABSSourceInstance('04_tw_accounts.csv.gz');
        self::assertSame('lf', $source->getLineEnding());
    }

    public function testGetLineEndingEmptyManifest(): void
    {
        $source = $this->createABSSourceInstance('empty.manifest', [], true);
        self::assertSame('lf', $source->getLineEnding());
    }
}
