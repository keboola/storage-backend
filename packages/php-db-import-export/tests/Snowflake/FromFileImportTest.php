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

        (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance($file)
        );

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

        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            'testingTableWithPrimaryKey',
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
        (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance($file)
        );

        $importOptions = new ImportOptions(
            self::SNOWFLAKE_SCHEMA_NAME,
            'testingTableWithPrimaryKey',
            [],
            $csvFile->getHeader(),
            true,
            false,
            ImportOptions::SKIP_FIRST_LINE
        );

        //import second file with some same data on primary key
        (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance('primaryKey/second.csv')
        );

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

        (new Importer($this->connection))->importTable(
            $importOptions,
            $this->createABSSourceInstance($file, true)
        );
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
