<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\SourceStorage\ABS;

use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage\ABS\SnowflakeAdapter;
use Keboola\Db\ImportExport\SourceStorage;
use PHPUnit\Framework\MockObject\MockObject;
use Tests\Keboola\Db\ImportExportUnit\BaseTestCase;

class SnowflakeAdapterTest extends BaseTestCase
{
    public function testExecuteCopyCommands(): void
    {
        /** @var SourceStorage\ABS\Source|MockObject $source */
        $source = self::createMock(SourceStorage\ABS\Source::class);
        $source->expects(self::once())->method('getCsvFile')->willReturn(new CsvFile(self::DATA_DIR . 'empty.csv'));
        /** @var Connection|MockObject $connection */
        $connection = self::createMock(Connection::class);
        $connection->expects(self::exactly(2))->method('fetchAll')->willReturn([['rows_loaded' => 1]]);
        /** @var ImportState|MockObject $state */
        $state = self::createMock(ImportState::class);
        $state->expects(self::once())->method('startTimer');
        $state->expects(self::once())->method('stopTimer');
        /** @var ImportOptions|MockObject $options */
        $options = self::createMock(ImportOptions::class);

        $adapter = new SnowflakeAdapter($source);
        $rows = $adapter->executeCopyCommands(
            ['cmd1', 'cmd2'],
            $connection,
            $options,
            $state
        );

        self::assertEquals(2, $rows);
    }

    public function testGetCopyCommands(): void
    {
        /** @var SourceStorage\ABS\Source|MockObject $source */
        $source = self::createMock(SourceStorage\ABS\Source::class);
        $source->expects(self::once())->method('getCsvFile')->willReturn(new CsvFile(self::DATA_DIR . 'empty.csv'));
        $source->expects(self::once())->method('getManifestEntries')->willReturn(['azure://url']);
        $source->expects(self::exactly(2))->method('getContainerUrl')->willReturn('containerUrl');
        $source->expects(self::once())->method('getSasToken')->willReturn('sasToken');

        $options = new ImportOptions('schema', 'table');
        $adapter = new SnowflakeAdapter($source);
        $commands = $adapter->getCopyCommands(
            $options,
            'stagingTable'
        );

        self::assertSame([
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ('azure://url')
EOT,

        ], $commands);
    }

    public function testGetCopyCommandsChunk(): void
    {
        $files = [];
        for ($i = 1; $i <= 1500; $i++) {
            $files[] = 'azure://url' . $i;
        }

        /** @var SourceStorage\ABS\Source|MockObject $source */
        $source = self::createMock(SourceStorage\ABS\Source::class);
        $source->expects(self::exactly(2))->method('getCsvFile')->willReturn(new CsvFile(self::DATA_DIR . 'empty.csv'));
        $source->expects(self::exactly(1))->method('getManifestEntries')->willReturn($files);
        $source->expects(self::exactly(1502/*Called for each entry plus 2times*/))
            ->method('getContainerUrl')->willReturn('containerUrl');
        $source->expects(self::exactly(2))->method('getSasToken')->willReturn('sasToken');

        $options = new ImportOptions('schema', 'table');
        $adapter = new SnowflakeAdapter($source);
        $commands = $adapter->getCopyCommands(
            $options,
            'stagingTable'
        );

        [$cmd1Files, $cmd2Files] = array_chunk($files, 1000);

        $cmd1Files = implode(', ', array_map(function ($file) {
            return sprintf("'%s'", $file);
        }, $cmd1Files));
        $cmd2Files = implode(', ', array_map(function ($file) {
            return sprintf("'%s'", $file);
        }, $cmd2Files));

        self::assertSame([
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd1Files)
EOT,
            <<<EOT
COPY INTO "schema"."stagingTable" 
FROM 'containerUrl'
CREDENTIALS=(AZURE_SAS_TOKEN='sasToken')
FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '\"' ESCAPE_UNENCLOSED_FIELD = NONE)
FILES = ($cmd2Files)
EOT,
        ], $commands);
    }
}
