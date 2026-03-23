<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\ToStage;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\FromABSCopyIntoAdapter;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockDbalConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class FromABSCopyIntoAdapterTest extends BaseTestCase
{
    use MockDbalConnectionTrait;

    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')
            ->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')
            ->willReturn(['azure://xxx.blob.core.windows.net/xx/xxx.csv']);
        $source->expects(self::any())->method('getContainerUrl')
            ->willReturn('azure://xxx.blob.core.windows.net/xx/');

        $conn = $this->mockConnection();
        // @codingStandardsIgnoreStart
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'azure://xxx.blob.core.windows.net/xx/'
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE, NULL_IF=(''))
FILES = ('xxx.csv')
EOT
        );
        // @codingStandardsIgnoreEnd
        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 10,
                    'name' => 'stagingTable',
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 10,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new SnowflakeImportOptions();
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsNullIfMultiple(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')
            ->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')
            ->willReturn(['azure://xxx.blob.core.windows.net/xx/xxx.csv']);
        $source->expects(self::any())->method('getContainerUrl')
            ->willReturn('azure://xxx.blob.core.windows.net/xx/');

        $conn = $this->mockConnection();
        // @codingStandardsIgnoreStart
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'azure://xxx.blob.core.windows.net/xx/'
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE, NULL_IF=('','NULL'))
FILES = ('xxx.csv')
EOT
        );
        // @codingStandardsIgnoreEnd
        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 10,
                    'name' => 'stagingTable',
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 10,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new SnowflakeImportOptions(
            importAsNull: ['', 'NULL'],
        );
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsNullIfEmpty(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')
            ->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')
            ->willReturn(['azure://xxx.blob.core.windows.net/xx/xxx.csv']);
        $source->expects(self::any())->method('getContainerUrl')
            ->willReturn('azure://xxx.blob.core.windows.net/xx/');

        $conn = $this->mockConnection();
        // @codingStandardsIgnoreStart
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'azure://xxx.blob.core.windows.net/xx/'
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE, NULL_IF=())
FILES = ('xxx.csv')
EOT
        );
        // @codingStandardsIgnoreEnd
        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 10,
                    'name' => 'stagingTable',
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 10,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new SnowflakeImportOptions(
            importAsNull: [],
        );
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(10, $count);
    }

    public function testGetCopyCommandsRowSkip(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')
            ->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')
            ->willReturn(['azure://xxx.blob.core.windows.net/xx/xxx.csv']);
        $source->expects(self::any())->method('getContainerUrl')
            ->willReturn('azure://xxx.blob.core.windows.net/xx/');

        $conn = $this->mockConnection();
        // @codingStandardsIgnoreStart
        $conn->expects(self::once())->method('executeStatement')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'azure://xxx.blob.core.windows.net/xx/'
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' SKIP_HEADER = 3 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE, NULL_IF=(''))
FILES = ('xxx.csv')
EOT
        );
        // @codingStandardsIgnoreEnd

        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 7,
                    'name' => 'stagingTable',
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 7,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new SnowflakeImportOptions([], false, false, 3);
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(7, $count);
    }

    public function testGetCopyCommandWithMoreChunksOfFiles(): void
    {
        $entries = [];
        $entriesWithoutBucket = [];

        // limit for snflk files in one query is 1000
        for ($i = 1; $i < 1005; $i++) {
            $entries[] = "azure://xxx.blob.core.windows.net/xx/file{$i}.csv";
            $entriesWithoutBucket[] = "'file{$i}.csv'";
        }

        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects(self::any())->method('getCsvOptions')
            ->willReturn(new CsvOptions());
        $source->expects(self::once())->method('getManifestEntries')
            ->willReturn($entries);
        $source->expects(self::any())->method('getContainerUrl')
            ->willReturn('azure://xxx.blob.core.windows.net/xx/');

        $conn = $this->mockConnection();

        // @codingStandardsIgnoreStart
        $qTemplate = <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'azure://xxx.blob.core.windows.net/xx/'
CREDENTIALS=(AZURE_SAS_TOKEN='')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE, NULL_IF=(''))
FILES = (%s)
EOT;
        // @codingStandardsIgnoreEnd
        $q1 = sprintf($qTemplate, implode(', ', array_slice($entriesWithoutBucket, 0, 1000)));
        $q2 = sprintf($qTemplate, implode(', ', array_slice($entriesWithoutBucket, 1000, 5)));
        $conn->expects(self::exactly(2))->method('executeStatement')->withConsecutive([$q1], [$q2]);

        $conn->expects(self::once())->method('fetchAllAssociative')
            ->willReturn([
                [
                    'TABLE_TYPE' => 'BASE TABLE',
                    'BYTES' => 0,
                    'ROW_COUNT' => 7,
                    'name' => 'stagingTable',
                    'kind' => 'BASE TABLE',
                    'bytes' => 0,
                    'rows' => 7,
                ],
            ]);

        $destination = new SnowflakeTableDefinition(
            'schema',
            'stagingTable',
            true,
            new ColumnCollection([]),
            [],
        );
        $options = new SnowflakeImportOptions();
        $adapter = new FromABSCopyIntoAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
        );

        self::assertEquals(7, $count);
    }
}
