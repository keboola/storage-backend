<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Tests\Keboola\Db\ImportExport\ImportExportBaseTest;

class FromFileImportTest extends ImportExportBaseTest
{
    public function testImportFile(): void
    {
        $file = 'file.csv';
        $csvFile = new CsvFile(self::DATA_DIR . $file);
        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            'testingTable',
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
            $this->createABSSourceInstance($file)
        );

        $this->assertCount(2, $result->getTimers());
        $this->assertEquals('copyToStaging-file.csv', $result->getTimers()[0]['name']);
        $this->assertEquals('copyFromStagingToTarget', $result->getTimers()[1]['name']);
        $this->assertEquals(1, $result->getImportedRowsCount());

        $this->assertTableEqualsFiles(
            $importOptions->getTableName(),
            [
                __DIR__ . '/../data/file.csv',
            ],
            'a',
            'Imported tables are not the same as files'
        );
    }

    public function testImportFileWithPrimaryKey(): void
    {
        $file = 'primaryKey/first.csv';
        $csvFile = new CsvFile(self::DATA_DIR . $file);
        $targetTable = 'testingTableWithPrimaryKey';

        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            $targetTable,
            [],
            $csvFile->getHeader(),
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );

        $this->createTableInSnowflake(
            $this->connection,
            $importOptions,
            [$csvFile->getHeader()[0]]
        );

        //import first file
        $result = (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance($file)
        );

        $this->assertCount(3, $result->getTimers());
        $this->assertEquals('copyToStaging-first.csv', $result->getTimers()[0]['name']);
        $this->assertEquals('dedup', $result->getTimers()[1]['name']);
        $this->assertEquals('copyFromStagingToTarget', $result->getTimers()[2]['name']);
        $this->assertEquals(3, $result->getImportedRowsCount());
        $this->assertSame($importOptions->getColumns(), $result->getImportedColumns());

        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            $targetTable,
            [],
            $csvFile->getHeader(),
            true,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );

        //import second file with some same data on primary key
        $result = (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance('primaryKey/second.csv')
        );
        $this->assertCount(5, $result->getTimers());
        $this->assertEquals('copyToStaging-second.csv', $result->getTimers()[0]['name']);
        $this->assertEquals('updateTargetTable', $result->getTimers()[1]['name']);
        $this->assertEquals('deleteUpdatedRowsFromStaging', $result->getTimers()[2]['name']);
        $this->assertEquals('dedupStaging', $result->getTimers()[3]['name']);
        $this->assertEquals('insertIntoTargetFromStaging', $result->getTimers()[4]['name']);
        $this->assertEquals(2, $result->getImportedRowsCount());
        $this->assertSame($importOptions->getColumns(), $result->getImportedColumns());

        $this->assertTableEqualsFiles(
            $importOptions->getTableName(),
            [
                __DIR__ . '/../data/primaryKey/result.csv',
            ],
            'a',
            'Imported tables are not the same as files'
        );
    }

    public function testImportSlicedFile(): void
    {
        $file = 'sliced/sliced.csvmanifest';
        $csvFile = new CsvFile(self::DATA_DIR . 'sliced/sliced.csv_01');
        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            'testingSlicedTable',
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
            $this->createABSSourceInstance($file, true)
        );

        $this->assertCount(2, $result->getTimers());
        $this->assertEquals('copyToStaging-sliced.csvmanifest', $result->getTimers()[0]['name']);
        $this->assertEquals('copyFromStagingToTarget', $result->getTimers()[1]['name']);
        $this->assertEquals(6, $result->getImportedRowsCount());
        $this->assertSame($importOptions->getColumns(), $result->getImportedColumns());

        $this->assertTableEqualsFiles(
            $importOptions->getTableName(),
            [
                __DIR__ . '/../data/sliced/sliced.csv_00',
                __DIR__ . '/../data/sliced/sliced.csv_01',
                __DIR__ . '/../data/sliced/sliced.csv_02',
            ],
            'a',
            'Imported tables are not the same as files'
        );
    }
}
