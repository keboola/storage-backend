<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage\Snowflake\Source;
use Tests\Keboola\Db\ImportExport\ImportExportBaseTest;

class FromSnowflakeImportTest extends ImportExportBaseTest
{
    public function testImportTable(): void
    {
        $file = 'file.csv';
        $csvFile = new CsvFile(self::DATA_DIR . $file);
        $source = new Source(self::SNOWFLAKE_SCHEMA_NAME, 'sourceTestingTable');

        $this->importFileToSnowflake($file, $source->getTableName());
        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            'targetTestingTable',
            [],
            $csvFile->getHeader(),
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );

        $this->createTableInSnowflake(
            $this->connection,
            $importOptions
        );

        $result = (new Importer($this->connection))->importTable(
            $importOptions,
            $source
        );

        $this->assertCount(2, $result->getTimers());
        $this->assertEquals('copyToStaging', $result->getTimers()[0]['name']);
        $this->assertEquals('copyFromStagingToTarget', $result->getTimers()[1]['name']);
        $this->assertEquals(1, $result->getImportedRowsCount());
        $this->assertSame($importOptions->getColumns(), $result->getImportedColumns());

        $this->assertTableEqualsFiles(
            $importOptions->getTableName(),
            [
                __DIR__ . '/../data/file.csv',
            ],
            'a',
            'Imported tables are not the same as files'
        );
    }
}
