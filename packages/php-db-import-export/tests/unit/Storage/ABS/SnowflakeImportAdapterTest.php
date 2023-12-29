<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Storage\ABS;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\ABS\SnowflakeImportAdapter;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportCommon\ABSSourceTrait;
use Tests\Keboola\Db\ImportExportUnit\Backend\Snowflake\MockConnectionTrait;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeImportAdapterTest extends BaseTestCase
{
    use MockConnectionTrait;
    use ABSSourceTrait;

    public function testIsSupported(): void
    {
        $absSource = $this->createDummyABSSourceInstance('');
        $snowflakeTable = new Storage\Snowflake\Table('', '');
        $snowflakeSelectSource = new Storage\Snowflake\SelectSource('', []);
        $synapseTable = new Storage\Synapse\Table('', '');

        $this->assertTrue(
            SnowflakeImportAdapter::isSupported(
                $absSource,
                $snowflakeTable,
            ),
        );

        $this->assertFalse(
            SnowflakeImportAdapter::isSupported(
                $snowflakeSelectSource,
                $snowflakeTable,
            ),
        );

        $this->assertFalse(
            SnowflakeImportAdapter::isSupported(
                $absSource,
                $synapseTable,
            ),
        );
    }

    public function testGetCopyCommands(): void
    {
        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->once())->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->once())->method('getManifestEntries')->willReturn(['azure://url']);
        $source->expects($this->exactly(2))->method('getContainerUrl')->willReturn('containerUrl');
        $source->expects($this->once())->method('getSasToken')->willReturn('sasToken');

        $conn = $this->mockConnection();
        $conn->expects($this->once())->method('query')->with(
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ('azure://url')
EOT,
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS "count" FROM "schema"."stagingTable"')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ],
            );

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SnowflakeImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable',
        );
        $this->assertEquals(10, $count);
    }

    public function testGetCopyCommandsChunk(): void
    {
        $files = [];
        for ($i = 1; $i <= 1500; $i++) {
            $files[] = 'azure://url' . $i;
        }

        /** @var Storage\ABS\SourceFile|MockObject $source */
        $source = $this->createMock(Storage\ABS\SourceFile::class);
        $source->expects($this->exactly(2))->method('getCsvOptions')->willReturn(new CsvOptions());
        $source->expects($this->exactly(1))->method('getManifestEntries')->willReturn($files);
        $source->expects($this->exactly(1502/*Called for each entry plus 2times*/))
            ->method('getContainerUrl')->willReturn('containerUrl');
        $source->expects($this->exactly(2))->method('getSasToken')->willReturn('sasToken');

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
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd1Files)
EOT
                ,
            ],
            [
                <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd2Files)
EOT
                ,
            ],
        );
        $conn->expects($this->once())->method('fetchAll')
            ->with('SELECT COUNT(*) AS "count" FROM "schema"."stagingTable"')
            ->willReturn(
                [
                    [
                        'count' => 10,
                    ],
                ],
            );

        $destination = new Storage\Snowflake\Table('schema', 'table');
        $options = new ImportOptions();
        $adapter = new SnowflakeImportAdapter($conn);
        $count = $adapter->runCopyCommand(
            $source,
            $destination,
            $options,
            'stagingTable',
        );
        $this->assertEquals(10, $count);
    }
}
