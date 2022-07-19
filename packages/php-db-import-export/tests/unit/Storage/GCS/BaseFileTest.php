<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\GCS;

use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class BaseFileTest extends BaseTestCase
{
    public function testDefaultValues(): void
    {
        $baseFile = new class(
            'bucket',
            'file.csv',
            'integration'
        ) extends Storage\GCS\BaseFile {
        };
        self::assertEquals('file.csv', $baseFile->getFilePath());
        self::assertEquals('integration', $baseFile->getStorageIntegrationName());
        self::assertEquals('bucket', $baseFile->getBucket());
        self::assertEquals('gcs://bucket', $baseFile->getGcsPrefix());
    }
}
