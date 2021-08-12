<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\S3;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\ExportOptions;
use PHPUnit\Framework\MockObject\MockObject;
use Keboola\Db\ImportExport\Storage;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class ExasolExportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommand(): void
    {
        $destination = new Storage\S3\DestinationFile(
            's3Key',
            's3Secret',
            'eu-central-1',
            'myBucket',
            'file.csv'
        );

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
EXPORT SELECT * FROM "schema"."table" INTO CSV AT 'https://myBucket.s3.eu-central-1.amazonaws.com' 
USER 's3Key' IDENTIFIED BY 's3Secret'
FILE 'file_001.csv'
FILE 'file_002.csv'
FILE 'file_003.csv'
FILE 'file_004.csv'
FILE 'file_005.csv'
FILE 'file_006.csv'
FILE 'file_007.csv'
FILE 'file_008.csv'
FILE 'file_009.csv'
FILE 'file_010.csv'
FILE 'file_011.csv'
FILE 'file_012.csv'
FILE 'file_013.csv'
FILE 'file_014.csv'
FILE 'file_015.csv'
FILE 'file_016.csv'
FILE 'file_017.csv'
FILE 'file_018.csv'
FILE 'file_019.csv'
FILE 'file_020.csv'
FILE 'file_021.csv'
FILE 'file_022.csv'
FILE 'file_023.csv'
FILE 'file_024.csv'
FILE 'file_025.csv'
FILE 'file_026.csv'
FILE 'file_027.csv'
FILE 'file_028.csv'
FILE 'file_029.csv'
FILE 'file_030.csv'
FILE 'file_031.csv'
FILE 'file_032.csv'
REPLACE
EOT
            ,
            []
        )->willReturn([]);

        $source = new Storage\Exasol\Table('schema', 'table');
        $options = new ExportOptions();
        $adapter = new Storage\S3\ExasolExportAdapter($conn);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options
            )
        );
    }

    public function testGetCopyCommandCompressed(): void
    {
        $destination = new Storage\S3\DestinationFile(
            's3Key',
            's3Secret',
            'eu-central-1',
            'myBucket',
            'file.csv'
        );

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
EXPORT SELECT * FROM "schema"."table" INTO CSV AT 'https://myBucket.s3.eu-central-1.amazonaws.com' 
USER 's3Key' IDENTIFIED BY 's3Secret'
FILE 'file_001.csv.gz.gz'
FILE 'file_002.csv.gz.gz'
FILE 'file_003.csv.gz.gz'
FILE 'file_004.csv.gz.gz'
FILE 'file_005.csv.gz.gz'
FILE 'file_006.csv.gz.gz'
FILE 'file_007.csv.gz.gz'
FILE 'file_008.csv.gz.gz'
FILE 'file_009.csv.gz.gz'
FILE 'file_010.csv.gz.gz'
REPLACE
EOT
            ,
            []
        )->willReturn([]);

        $source = new Storage\Exasol\Table('schema', 'table');
        $options = new ExportOptions(true);
        $adapter = new Storage\S3\ExasolExportAdapter($conn);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options
            )
        );
    }

    public function testGetCopyCommandQuery(): void
    {
        $destination = new Storage\S3\DestinationFile(
            's3Key',
            's3Secret',
            'eu-central-1',
            'myBucket',
            'file.csv'
        );

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
EXPORT SELECT * FROM "schema"."table" INTO CSV AT 'https://myBucket.s3.eu-central-1.amazonaws.com' 
USER 's3Key' IDENTIFIED BY 's3Secret'
FILE 'file_001.csv'
FILE 'file_002.csv'
FILE 'file_003.csv'
FILE 'file_004.csv'
FILE 'file_005.csv'
FILE 'file_006.csv'
FILE 'file_007.csv'
FILE 'file_008.csv'
FILE 'file_009.csv'
FILE 'file_010.csv'
FILE 'file_011.csv'
FILE 'file_012.csv'
FILE 'file_013.csv'
FILE 'file_014.csv'
FILE 'file_015.csv'
FILE 'file_016.csv'
FILE 'file_017.csv'
FILE 'file_018.csv'
FILE 'file_019.csv'
FILE 'file_020.csv'
FILE 'file_021.csv'
FILE 'file_022.csv'
FILE 'file_023.csv'
FILE 'file_024.csv'
FILE 'file_025.csv'
FILE 'file_026.csv'
FILE 'file_027.csv'
FILE 'file_028.csv'
FILE 'file_029.csv'
FILE 'file_030.csv'
FILE 'file_031.csv'
FILE 'file_032.csv'
REPLACE
EOT
            ,
            []
        )->willReturn([]);

        $source = new Storage\Exasol\SelectSource('SELECT * FROM "schema"."table"');
        $options = new ExportOptions();
        $adapter = new Storage\S3\ExasolExportAdapter($conn);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options
            )
        );
    }
}
