<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\S3;

use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class DestinationFileTest extends BaseTestCase
{
    public function testDefaultValues(): void
    {
        $destination = new Storage\S3\DestinationFile(
            's3Key',
            's3Secret',
            'eu-central-1',
            'myBucket',
            'file.csv'
        );
        self::assertInstanceOf(Storage\S3\DestinationFile::class, $destination);
        self::assertInstanceOf(Storage\DestinationInterface::class, $destination);
        self::assertEquals('eu-central-1', $destination->getRegion());
        self::assertEquals('s3Key', $destination->getKey());
        self::assertEquals('s3://myBucket', $destination->getS3Prefix());
        self::assertEquals('s3Secret', $destination->getSecret());
        $this->assertSame([
            'file_001.csv',
            'file_002.csv',
            'file_003.csv',
            'file_004.csv',
            'file_005.csv',
            'file_006.csv',
            'file_007.csv',
            'file_008.csv',
            'file_009.csv',
            'file_010.csv',
            'file_011.csv',
            'file_012.csv',
            'file_013.csv',
            'file_014.csv',
            'file_015.csv',
            'file_016.csv',
            'file_017.csv',
            'file_018.csv',
            'file_019.csv',
            'file_020.csv',
            'file_021.csv',
            'file_022.csv',
            'file_023.csv',
            'file_024.csv',
            'file_025.csv',
            'file_026.csv',
            'file_027.csv',
            'file_028.csv',
            'file_029.csv',
            'file_030.csv',
            'file_031.csv',
            'file_032.csv',
        ], $destination->getSlicedFilesNames(false));

        $this->assertSame([
            'file_001.csv.gz',
            'file_002.csv.gz',
            'file_003.csv.gz',
            'file_004.csv.gz',
            'file_005.csv.gz',
            'file_006.csv.gz',
            'file_007.csv.gz',
            'file_008.csv.gz',
            'file_009.csv.gz',
            'file_010.csv.gz',
        ], $destination->getSlicedFilesNames(true));
    }
}
