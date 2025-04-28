<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Exporter;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\Export\GcsExportAdapter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class GcsExportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommand(): void
    {
        /** @var Storage\GCS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getGcsPrefix')->willReturn('bucket');
        $destination->expects(self::once())->method('getFilePath')->willReturn('xxx/path');
        $destination->expects(self::once())->method('getStorageIntegrationName')->willReturn('STORAGE_INTEGRATION');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('executeQuery')->with(
            <<<EOT
COPY INTO 'bucket/xxx/path'
FROM (SELECT * FROM "schema"."table")
STORAGE_INTEGRATION = "STORAGE_INTEGRATION"
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='NONE'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS',
    NULL_IF = ()
)
MAX_FILE_SIZE=200000000
DETAILED_OUTPUT = TRUE
EOT
            ,
            [],
        );

        $conn->expects(self::once())->method('fetchAllAssociative')->with('select * from table(result_scan(last_query_id()));')
            ->willReturn([]);

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP);
        $adapter = new GcsExportAdapter($conn);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandCompressed(): void
    {
        /** @var Storage\GCS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getGcsPrefix')->willReturn('bucket');
        $destination->expects(self::once())->method('getFilePath')->willReturn('test/file');
        $destination->expects(self::once())->method('getStorageIntegrationName')->willReturn('STORAGE_INTEGRATION');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('executeQuery')->with(
            <<<EOT
COPY INTO 'bucket/test/file'
FROM (SELECT * FROM "schema"."table")
STORAGE_INTEGRATION = "STORAGE_INTEGRATION"
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='GZIP'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS',
    NULL_IF = ()
)
MAX_FILE_SIZE=200000000
DETAILED_OUTPUT = TRUE
EOT
            ,
            [],
        );

        $conn->expects(self::once())->method('fetchAllAssociative')->with('select * from table(result_scan(last_query_id()));')
            ->willReturn([]);

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(true, ExportOptions::MANIFEST_SKIP);
        $adapter = new GcsExportAdapter($conn);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandQuery(): void
    {
        /** @var Storage\GCS\DestinationFile|MockObject $destination */
        $destination = self::createMock(Storage\GCS\DestinationFile::class);
        $destination->expects(self::once())->method('getGcsPrefix')->willReturn('bucket');
        $destination->expects(self::once())->method('getFilePath')->willReturn('test/file');
        $destination->expects(self::once())->method('getStorageIntegrationName')->willReturn('STORAGE_INTEGRATION');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('executeQuery')->with(
            <<<EOT
COPY INTO 'bucket/test/file'
FROM (SELECT * FROM "schema"."tableName")
STORAGE_INTEGRATION = "STORAGE_INTEGRATION"
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='NONE'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS',
    NULL_IF = ()
)
MAX_FILE_SIZE=200000000
DETAILED_OUTPUT = TRUE
EOT
            ,
            [],
        );

        $conn->expects(self::once())->method('fetchAllAssociative')->with('select * from table(result_scan(last_query_id()));')
            ->willReturn([]);

        $source = new Storage\Snowflake\SelectSource('SELECT * FROM "schema"."tableName"');
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP);
        $adapter = new GcsExportAdapter($conn);

        $this->assertSame(
            [],
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }
}
