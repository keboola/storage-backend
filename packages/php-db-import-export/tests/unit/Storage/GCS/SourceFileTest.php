<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\GCS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportCommon\GCSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use GCSSourceTrait;

    public function testDefaultValues(): void
    {
        $source = $this->createDummyGCSSourceInstance('file.csv');
        self::assertInstanceOf(Storage\GCS\BaseFile::class, $source);
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertSame('file.csv', $source->getFilePath());
        self::assertSame('integration', $source->getStorageIntegrationName());
        self::assertSame('gcsBucket', $source->getBucket());
        self::assertSame('gcs://gcsBucket', $source->getGcsPrefix());
        self::assertSame('', $source->getPrefix());
        self::assertSame([], $source->getColumnsNames());
        self::assertNull($source->getPrimaryKeysNames());
        self::assertInstanceOf(CsvOptions::class, $source->getCsvOptions());
    }
}
