<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\Exporter;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake\Export\S3ExportAdapter;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class S3ExportAdapterTest extends BaseTestCase
{
    public function testGetCopyCommand(): void
    {
        $expectedCopyResult = [
            [
                'FILE_NAME' => 'containerUrl',
                'FILE_SIZE' => '0',
                'ROW_COUNT' => '123',
            ],
        ];

        /** @var Storage\S3\DestinationFile|MockObject $destination */
        $destination = $this->createMock(Storage\S3\DestinationFile::class);
        $destination->expects(self::once())->method('getFilePath')->willReturn('xxx/path');
        $destination->expects(self::once())->method('getKey')->willReturn('key');
        $destination->expects(self::once())->method('getSecret')->willReturn('secret');
        $destination->expects(self::once())->method('getRegion')->willReturn('region');
        $destination->expects(self::once())->method('getS3Prefix')->willReturn('s3://bucketUrl');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('fetchAllAssociative')->with(
            <<<EOT
COPY INTO 's3://bucketUrl/xxx/path'
FROM (SELECT * FROM "schema"."table")
CREDENTIALS = (
    AWS_KEY_ID = 'key'
    AWS_SECRET_KEY = 'secret'
)
REGION = 'region'
FILE_FORMAT = (
    TYPE = 'CSV' FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='NONE'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS',
    NULL_IF = ()
)
ENCRYPTION = (
    TYPE = 'AWS_SSE_S3'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=50000000
DETAILED_OUTPUT = TRUE
EOT
            ,
            [],
        )->willReturn($expectedCopyResult);

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP);
        $adapter = new S3ExportAdapter($conn);

        $this->assertSame(
            $expectedCopyResult,
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandCompressed(): void
    {
        $expectedCopyResult = [
            [
                'FILE_NAME' => 'containerUrl',
                'FILE_SIZE' => '0',
                'ROW_COUNT' => '123',
            ],
        ];

        /** @var Storage\S3\DestinationFile|MockObject $destination */
        $destination = $this->createMock(Storage\S3\DestinationFile::class);
        $destination->expects(self::once())->method('getFilePath')->willReturn('xxx/path');
        $destination->expects(self::once())->method('getKey')->willReturn('key');
        $destination->expects(self::once())->method('getSecret')->willReturn('secret');
        $destination->expects(self::once())->method('getRegion')->willReturn('region');
        $destination->expects(self::once())->method('getS3Prefix')->willReturn('s3://bucketUrl');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('fetchAllAssociative')->with(
            <<<EOT
COPY INTO 's3://bucketUrl/xxx/path'
FROM (SELECT * FROM "schema"."table")
CREDENTIALS = (
    AWS_KEY_ID = 'key'
    AWS_SECRET_KEY = 'secret'
)
REGION = 'region'
FILE_FORMAT = (
    TYPE = 'CSV' FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='GZIP'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS',
    NULL_IF = ()
)
ENCRYPTION = (
    TYPE = 'AWS_SSE_S3'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=50000000
DETAILED_OUTPUT = TRUE
EOT
            ,
            [],
        )->willReturn($expectedCopyResult);

        $source = new Storage\Snowflake\Table('schema', 'table');
        $options = new ExportOptions(true, ExportOptions::MANIFEST_SKIP);
        $adapter = new S3ExportAdapter($conn);

        $this->assertSame(
            $expectedCopyResult,
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }

    public function testGetCopyCommandQuery(): void
    {
        $expectedCopyResult = [
            [
                'FILE_NAME' => 'containerUrl',
                'FILE_SIZE' => '0',
                'ROW_COUNT' => '123',
            ],
        ];

        /** @var Storage\S3\DestinationFile|MockObject $destination */
        $destination = $this->createMock(Storage\S3\DestinationFile::class);
        $destination->expects(self::once())->method('getFilePath')->willReturn('xxx/path');
        $destination->expects(self::once())->method('getKey')->willReturn('key');
        $destination->expects(self::once())->method('getSecret')->willReturn('secret');
        $destination->expects(self::once())->method('getRegion')->willReturn('region');
        $destination->expects(self::once())->method('getS3Prefix')->willReturn('s3://bucketUrl');

        /** @var Connection|MockObject $conn */
        $conn = $this->createMock(Connection::class);
        $conn->expects(self::once())->method('fetchAllAssociative')->with(
            <<<EOT
COPY INTO 's3://bucketUrl/xxx/path'
FROM (SELECT * FROM "schema"."table")
CREDENTIALS = (
    AWS_KEY_ID = 'key'
    AWS_SECRET_KEY = 'secret'
)
REGION = 'region'
FILE_FORMAT = (
    TYPE = 'CSV' FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    COMPRESSION='NONE'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS',
    NULL_IF = ()
)
ENCRYPTION = (
    TYPE = 'AWS_SSE_S3'
)
OVERWRITE = TRUE
MAX_FILE_SIZE=50000000
DETAILED_OUTPUT = TRUE
EOT
            ,
            [],
        )->willReturn($expectedCopyResult);

        $source = new Storage\Snowflake\SelectSource('SELECT * FROM "schema"."table"');
        $options = new ExportOptions(false, ExportOptions::MANIFEST_SKIP);
        $adapter = new S3ExportAdapter($conn);

        $this->assertSame(
            $expectedCopyResult,
            $adapter->runCopyCommand(
                $source,
                $destination,
                $options,
            ),
        );
    }
}
