<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\S3;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3\SnowflakeImportAdapter;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportCommon\S3SourceTrait;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeImportAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;
    use S3SourceTrait;

    public function testIsSupported(): void
    {
        $s3Source = $this->createDummyS3SourceInstance('');
        $snowflakeTable = new Storage\Snowflake\Table('', '');
        $snowflakeSelectSource = new Storage\Snowflake\SelectSource('', []);
        $synapseTable = new Storage\Synapse\Table('', '');

        $this->assertTrue(
            SnowflakeImportAdapter::isSupported(
                $s3Source,
                $snowflakeTable
            )
        );

        $this->assertFalse(
            SnowflakeImportAdapter::isSupported(
                $snowflakeSelectSource,
                $snowflakeTable
            )
        );

        $this->assertFalse(
            SnowflakeImportAdapter::isSupported(
                $s3Source,
                $synapseTable
            )
        );
    }

    public function testGetCopyCommands(): void
    {
        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::once())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['s3://url']);
        $source->expects(self::exactly(2))->method('getS3Prefix')->willReturn('s3Prefix');
        $source->expects(self::once())->method('getKey')->willReturn('s3Key');
        $source->expects(self::once())->method('getSecret')->willReturn('s3Secret');
        $source->expects(self::once())->method('getRegion')->willReturn('s3Region');
        $conn = $this->mockConnection();
        $conn->expects(self::once())->method('query')->with(
            <<<EOT
COPY INTO "schema"."stagingTable"
FROM 's3Prefix' 
CREDENTIALS = (AWS_KEY_ID = 's3Key' AWS_SECRET_KEY = 's3Secret')
REGION = 's3Region'
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ('s3://url')
EOT
        );
        $conn->expects(self::once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS "count" FROM "schema"."stagingTable"')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SnowflakeImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
        );
        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsChunk(): void
    {
        $files = [];
        for ($i = 1; $i <= 1500; $i++) {
            $files[] = 's3://url' . $i;
        }

        /** @var Storage\S3\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\S3\SourceFile::class);
        $source->expects(self::exactly(2))->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')->willReturn($files);
        $source->method('getS3Prefix')->willReturn('s3Prefix');
        $source->method('getKey')->willReturn('s3Key');
        $source->method('getSecret')->willReturn('s3Secret');
        $source->method('getRegion')->willReturn('s3Region');

        [$cmd1Files, $cmd2Files] = array_chunk($files, 1000);

        $cmd1Files = implode(', ', array_map(static function ($file) {
            return sprintf("'%s'", $file);
        }, $cmd1Files));
        $cmd2Files = implode(', ', array_map(static function ($file) {
            return sprintf("'%s'", $file);
        }, $cmd2Files));

        $conn = $this->mockConnection();
        $conn->expects($this->exactly(2))->method('query')->withConsecutive(
            [
                <<<EOT
COPY INTO "schema"."stagingTable"
FROM 's3Prefix' 
CREDENTIALS = (AWS_KEY_ID = 's3Key' AWS_SECRET_KEY = 's3Secret')
REGION = 's3Region'
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd1Files)
EOT
                ,
            ],
            [
                <<<EOT
COPY INTO "schema"."stagingTable"
FROM 's3Prefix' 
CREDENTIALS = (AWS_KEY_ID = 's3Key' AWS_SECRET_KEY = 's3Secret')
REGION = 's3Region'
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd2Files)
EOT
                ,
            ]
        );
        $conn->expects(self::once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS "count" FROM "schema"."stagingTable"')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ]
            );

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SnowflakeImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable'
        );
        self::assertEquals(10, $count);
    }
}
