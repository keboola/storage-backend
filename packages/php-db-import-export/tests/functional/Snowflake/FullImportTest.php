<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class FullImportTest extends SnowflakeImportExportBaseTest
{
    public function fullImportData(): array
    {
        $expectedEscaping = [];
        $file = new CsvFile(self::DATA_DIR . 'escaping/standard-with-enclosures.csv');
        foreach ($file as $row) {
            $expectedEscaping[] = $row;
        }
        $escapingHeader = array_shift($expectedEscaping); // remove header
        $expectedEscaping = array_values($expectedEscaping);

        $expectedAccounts = [];
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        foreach ($file as $row) {
            $expectedAccounts[] = $row;
        }
        $accountsHeader = array_shift($expectedAccounts); // remove header
        $expectedAccounts = array_values($expectedAccounts);

        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();

        $file = new CsvFile(self::DATA_DIR . 'lemma.csv');
        $expectedLemma = [];
        foreach ($file as $row) {
            $expectedLemma[] = $row;
        }
        $lemmaHeader = array_shift($expectedLemma);
        $expectedLemma = array_values($expectedLemma);

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        $tests = [];

        // full imports
        $tests[] = [

            $this->createABSSourceInstance('sliced/2cols-large/2cols-large.csvmanifest', true),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader, ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501
        ];

        $tests[] = [
            $this->createABSSourceInstance('empty.manifest', true),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader, ImportOptions::SKIP_NO_LINE),
            [],
            0
        ];

        $tests[] = [
            $this->createABSSourceInstance('lemma.csv'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.lemma'),
            $this->getSimpleImportOptions($lemmaHeader),
            $expectedLemma,
            5
        ];

        $tests[] = [
            $this->createABSSourceInstance('standard-with-enclosures.csv'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7
        ];

        $tests[] = [
            $this->createABSSourceInstance('gzipped-standard-with-enclosures.csv.gz'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7
        ];

        $tests[] = [
            $this->createABSSourceInstanceFromCsv(
                new CsvFile('standard-with-enclosures.tabs.csv', "\t")
            ),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7
        ];

        $tests[] = [
            $this->createABSSourceInstanceFromCsv(new CsvFile('raw.rs.csv', "\t", '', '\\')),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7
        ];

        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.changedColumnsOrder.csv'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3'),
            $this->getSimpleImportOptions($accountChangedColumnsOrderHeader),
            $expectedAccounts,
            3
        ];
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3'),
            $this->getSimpleImportOptions($accountsHeader),
            $expectedAccounts,
            3
        ];
        // manifests
        $tests[] = [
            $this->createABSSourceInstance('sliced/accounts/accounts.csvmanifest', true),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3'),
            $this->getSimpleImportOptions($accountsHeader, ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3
        ];

        $tests[] = [
            $this->createABSSourceInstance('sliced/accounts-gzip/accounts-gzip.csvmanifest', true),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'accounts-3'),
            $this->getSimpleImportOptions($accountsHeader, ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3
        ];

        // reserved words
        $tests[] = [
            $this->createABSSourceInstance('reserved-words.csv', false),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'table'),
            $this->getSimpleImportOptions(['column', 'table']),
            [['table', 'column']],
            1
        ];
        // import table with _timestamp columns - used by snapshots
        $tests[] = [
            $this->createABSSourceInstance('with-ts.csv', false),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions([
                'col1',
                'col2',
                '_timestamp',
            ]),
            [
                ['a', 'b', '2014-11-10 13:12:06.000'],
                ['c', 'd', '2014-11-10 14:12:06.000'],
            ],
            2
        ];
        // test creating table without _timestamp column
        $tests[] = [
            $this->createABSSourceInstance('standard-with-enclosures.csv', false),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.no_timestamp_table'),
            new ImportOptions(
                [],
                $escapingHeader,
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $expectedEscaping,
            7
        ];
        // copy from table
        $tests[] = [
            new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'out.csv_2Cols'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'out.csv_2Cols'),
            $this->getSimpleImportOptions($escapingHeader),
            [['a', 'b'], ['c', 'd']],
            2
        ];
        $tests[] = [
            new Storage\Snowflake\Table(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'types'),
            new Storage\Snowflake\Table(self::SNOWFLAKE_DEST_SCHEMA_NAME, 'types'),
            $this->getSimpleImportOptions([
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
            ]),
            [['a', '10.5', '0.3', 'true']],
            1
        ];

        return $tests;
    }

    /**
     * @dataProvider  fullImportData
     * @param Storage\Snowflake\Table $destination
     */
    public function testFullImport(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options,
        array $expected,
        int $expectedImportedRowCount
    ): void {
        $result = (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options
        );

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());
        $this->assertTableEqualsExpected(
            $destination,
            $options,
            $expected,
            0
        );
    }
}
