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
            'file.csv_001.csv',
            'file.csv_002.csv',
            'file.csv_003.csv',
            'file.csv_004.csv',
            'file.csv_005.csv',
            'file.csv_006.csv',
            'file.csv_007.csv',
            'file.csv_008.csv',
            'file.csv_009.csv',
            'file.csv_010.csv',
            'file.csv_011.csv',
            'file.csv_012.csv',
            'file.csv_013.csv',
            'file.csv_014.csv',
            'file.csv_015.csv',
            'file.csv_016.csv',
            'file.csv_017.csv',
            'file.csv_018.csv',
            'file.csv_019.csv',
            'file.csv_020.csv',
            'file.csv_021.csv',
            'file.csv_022.csv',
            'file.csv_023.csv',
            'file.csv_024.csv',
            'file.csv_025.csv',
            'file.csv_026.csv',
            'file.csv_027.csv',
            'file.csv_028.csv',
            'file.csv_029.csv',
            'file.csv_030.csv',
            'file.csv_031.csv',
            'file.csv_032.csv',
        ], $destination->getSlicedFilesNames(false));

        $this->assertSame([
            'file.csv_001.csv.gz',
            'file.csv_002.csv.gz',
            'file.csv_003.csv.gz',
            'file.csv_004.csv.gz',
            'file.csv_005.csv.gz',
            'file.csv_006.csv.gz',
            'file.csv_007.csv.gz',
            'file.csv_008.csv.gz',
            'file.csv_009.csv.gz',
            'file.csv_010.csv.gz',
        ], $destination->getSlicedFilesNames(true));
    }
}
