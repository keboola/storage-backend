<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\FromS3CopyIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockDbalConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromS3CopyIntoAdapterTest extends BaseTestCase
{
    use MockDbalConnectionTrait;

    public function testGetCopyCommands(): void
    {
        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" FROM '' 
                CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
                REGION = ''
                FILE_FORMAT = (TYPE=CSV 
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 0,
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
        ESCAPE_UNENCLOSED_FIELD = NONE
        )
                FILES = ('https:url')
EOT
        );

        $conn->expects(self::once())->method('fetchAllAssociative')
            // phpcs:ignore
            ->with("SELECT TABLE_TYPE,BYTES,ROW_COUNT FROM information_schema.tables WHERE TABLE_SCHEMA = 'schema' AND TABLE_NAME = 'stagingTable';")
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE', 'BYTES' => 0, 'ROW_COUNT' => 10,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            []
        );
        $options = new SnowflakeImportOptions();
        $adapter = new FromS3CopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsRowSkip(): void
    {
        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['https://url']);

        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" FROM '' 
                CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
                REGION = ''
                FILE_FORMAT = (TYPE=CSV 
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 3,
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
        ESCAPE_UNENCLOSED_FIELD = NONE
        )
                FILES = ('https:url')
EOT
        );

        $conn->expects(self::once())->method('fetchAllAssociative')
            // phpcs:ignore
            ->with("SELECT TABLE_TYPE,BYTES,ROW_COUNT FROM information_schema.tables WHERE TABLE_SCHEMA = 'schema' AND TABLE_NAME = 'stagingTable';")
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE', 'BYTES' => 0, 'ROW_COUNT' => 7,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            []
        );
        $options = new SnowflakeImportOptions([], false, false, 3);
        $adapter = new FromS3CopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options
        );

        self::assertEquals(7, $count);
    }

    public function testGetCopyCommandWithMoreChunksOfFiles(): void
    {
        $entries = [];
        $entriesWithoutBucket = [];

        // limit for snflk files in one query is 1000
        for ($i = 1; $i < 1005; $i++) {
            $entries[] = "s3://bucket/file{$i}";
            $entriesWithoutBucket[] = "'file{$i}'";
        }

        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn($entries);
        $source->expects(self::exactly(2))->method('getS3Prefix')->willReturn('s3://bucket');

        $conn = $this->mockConnection();

        $qTemplate = <<<EOT
COPY INTO "schema"."stagingTable" FROM 's3://bucket' 
                CREDENTIALS = (AWS_KEY_ID = '' AWS_SECRET_KEY = '')
                REGION = ''
                FILE_FORMAT = (TYPE=CSV 
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 0,
        FIELD_OPTIONALLY_ENCLOSED_BY = '\"',
        ESCAPE_UNENCLOSED_FIELD = NONE
        )
                FILES = (%s)
EOT;
        $q1 = sprintf($qTemplate, implode(', ', array_slice($entriesWithoutBucket, 0, 1000)));
        $q2 = sprintf($qTemplate, implode(', ', array_slice($entriesWithoutBucket, 1000, 5)));
        $conn->expects(self::exactly(2))->method('executeStatement')->withConsecutive([$q1], [$q2]);

        $conn->expects(self::once())->method('fetchAllAssociative')
            // phpcs:ignore
            ->with("SELECT TABLE_TYPE,BYTES,ROW_COUNT FROM information_schema.tables WHERE TABLE_SCHEMA = 'schema' AND TABLE_NAME = 'stagingTable';")
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE', 'BYTES' => 0, 'ROW_COUNT' => 7,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            []
        );
        $options = new SnowflakeImportOptions();
        $adapter = new FromS3CopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options
        );

        self::assertEquals(7, $count);
    }
}
