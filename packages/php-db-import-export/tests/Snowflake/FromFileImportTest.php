<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExport\Snowflake;

use DateTime;
use Keboola\Csv\CsvFile;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\Snowflake;
use Keboola\Db\ImportExport\ImportOptions;
use MicrosoftAzure\Storage\Blob\BlobSharedAccessSignatureHelper;
use MicrosoftAzure\Storage\Common\Internal\Resources;
use PHP_CodeSniffer\Reports\Csv;
use PHPUnit\Framework\TestCase;
use Keboola\Db\ImportExport\File;
use Tests\Keboola\Db\ImportExport\ImportExportBaseTest;

class FromFileImportTest extends ImportExportBaseTest
{

    private const DATA_DIR = __DIR__ . '/../data/';

    public function testImportFile(): void
    {
        $file = 'file.csv';
        $csvFile = new CsvFile(self::DATA_DIR . $file);
        $this->createTableInSnowflake(
            $this->connection,
            'testingTable',
            $csvFile->getHeader()
        );

        $importOptions = new ImportOptions(self::SNOWFLAKE_SCHEMA_NAME, 'testingTable');
        $importOptions->setNumberOfIgnoredLines(ImportOptions::SKIP_FIRST_LINE);
        $importOptions->setColumns($csvFile->getHeader());

        (new Snowflake($this->connection))->importTableFromFile(
            $importOptions,
            $this->createAzureFileInstance($file)
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

    public function testImportFileWithPrivateKey(): void
    {
        $file = 'primaryKey/first.csv';
        $csvFile = new CsvFile(self::DATA_DIR . $file);
        $this->createTableInSnowflake(
            $this->connection,
            'testingTableWithPrimaryKey',
            $csvFile->getHeader(),
            [$csvFile->getHeader()[0]]
        );

        $importOptions = new ImportOptions(self::SNOWFLAKE_SCHEMA_NAME, 'testingTableWithPrimaryKey');
        $importOptions->setColumns($csvFile->getHeader());
        $importOptions->getNumberOfIgnoredLines(ImportOptions::SKIP_FIRST_LINE);
        //import first file
        (new Snowflake($this->connection))->importTableFromFile(
            $importOptions,
            $this->createAzureFileInstance($file)
        );
        //import second file with some same data on primary key
        (new Snowflake($this->connection))->importTableFromFile(
            $importOptions,
            $this->createAzureFileInstance('primaryKey/second.csv')
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
        $this->createTableInSnowflake(
            $this->connection,
            'testingSlicedTable',
            $csvFile->getHeader()
        );

        $importOptions = new ImportOptions(self::SNOWFLAKE_SCHEMA_NAME, 'testingSlicedTable');
        $importOptions->setNumberOfIgnoredLines(ImportOptions::SKIP_FIRST_LINE);
        $importOptions->setColumns($csvFile->getHeader());
        (new Snowflake($this->connection))->importTableFromFile(
            $importOptions,
            $this->createAzureFileInstance($file, File\Azure::IS_SLICED)
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
