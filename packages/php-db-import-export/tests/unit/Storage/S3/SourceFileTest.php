<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\S3;

use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportCommon\S3SourceTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SourceFileTest extends BaseTestCase
{
    use S3SourceTrait;

    public function testDefaultValues(): void
    {
        $source = $this->createDummyS3SourceInstance('file.csv');
        self::assertInstanceOf(Storage\S3\SourceFile::class, $source);
        self::assertInstanceOf(Storage\SourceInterface::class, $source);
        self::assertEquals('eu-central-1', $source->getRegion());
        self::assertEquals('s3Key', $source->getKey());
        self::assertEquals('s3://myBucket', $source->getS3Prefix());
        self::assertEquals('s3Secret', $source->getSecret());
        self::assertEquals([], $source->getColumnsNames());
        self::assertNull($source->getPrimaryKeysNames());
    }

    public function testGetFilepathParts(): void
    {
        $source = $this->createDummyS3SourceInstance('data/shared/file.csv');
        self::assertEquals('data/shared/', $source->getPrefix());
        self::assertEquals('file.csv', $source->getFileName());
    }
}
