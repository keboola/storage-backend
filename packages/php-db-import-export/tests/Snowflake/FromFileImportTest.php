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
    public function testImportFile(): void
    {
        $prefix = __DIR__ . '/../data/';
        $file = 'file.csv';
        $csvFile = new CsvFile($prefix . $file);
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
            new File\Azure(
                (string) getenv('ABS_CONTAINER_NAME'),
                $file,
                $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
                (string) getenv('ABS_ACCOUNT_NAME'),
                $csvFile,
                false
            )
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

    public function testImportSlicedFile(): void
    {
        $file = 'sliced/sliced.csvmanifest';
        $csvFile = new CsvFile(__DIR__ . '/../data/sliced/sliced.csv_01');
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
            new File\Azure(
                (string) getenv('ABS_CONTAINER_NAME'),
                $file,
                $this->getCredentialsForAzureContainer((string) getenv('ABS_CONTAINER_NAME')),
                (string) getenv('ABS_ACCOUNT_NAME'),
                new CsvFile($file),
                true
            )
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
