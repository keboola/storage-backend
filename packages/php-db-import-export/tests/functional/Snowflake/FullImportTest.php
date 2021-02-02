<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
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

            $this->createABSSourceInstance('sliced/2cols-large/2cols-large.csvmanifest', $escapingHeader, true),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
        ];

        $tests[] = [
            $this->createABSSourceInstance('empty.manifest', $escapingHeader, true),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
        ];

        $tests[] = [
            $this->createABSSourceInstance('lemma.csv', $lemmaHeader),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.lemma'),
            $this->getSimpleImportOptions(),
            $expectedLemma,
            5,
        ];

        $tests[] = [
            $this->createABSSourceInstance('standard-with-enclosures.csv', $escapingHeader),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            $this->createABSSourceInstance('gzipped-standard-with-enclosures.csv.gz', $escapingHeader),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            $this->createABSSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingHeader
            ),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            $this->createABSSourceInstanceFromCsv('raw.rs.csv', new CsvOptions("\t", '', '\\'), $escapingHeader),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.changedColumnsOrder.csv', $accountChangedColumnsOrderHeader),
            new Storage\Snowflake\Table(
                $this->getDestinationSchemaName(),
                'accounts-3'
            ),
            $this->getSimpleImportOptions(),
            $expectedAccounts,
            3,
        ];
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv', $accountsHeader),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'accounts-3'),
            $this->getSimpleImportOptions(),
            $expectedAccounts,
            3,
        ];
        // manifests
        $tests[] = [
            $this->createABSSourceInstance('sliced/accounts/accounts.csvmanifest', $accountsHeader, true),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'accounts-3'),
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
        ];

        $tests[] = [
            $this->createABSSourceInstance('sliced/accounts-gzip/accounts-gzip.csvmanifest', $accountsHeader, true),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'accounts-3'),
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
        ];

        // folder
        $tests[] = [
            $this->createABSSourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsHeader,
                true,
                true
            ),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'accounts-3'),
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
        ];

        // reserved words
        $tests[] = [
            $this->createABSSourceInstance('reserved-words.csv', ['column', 'table'], false),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'table'),
            $this->getSimpleImportOptions(),
            [['table', 'column']],
            1,
        ];
        // import table with _timestamp columns - used by snapshots
        $tests[] = [
            $this->createABSSourceInstance(
                'with-ts.csv',
                [
                    'col1',
                    'col2',
                    '_timestamp',
                ],
                false
            ),
            new Storage\Snowflake\Table(
                $this->getDestinationSchemaName(),
                'out.csv_2Cols'
            ),
            $this->getSimpleImportOptions(),
            [
                ['a', 'b', '2014-11-10 13:12:06.000'],
                ['c', 'd', '2014-11-10 14:12:06.000'],
            ],
            2,
        ];
        // test creating table without _timestamp column
        $tests[] = [
            $this->createABSSourceInstance('standard-with-enclosures.csv', $escapingHeader, false),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.no_timestamp_table'),
            new ImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $expectedEscaping,
            7,
        ];
        // copy from table
        $tests[] = [
            new Storage\Snowflake\Table($this->getSourceSchemaName(), 'out.csv_2Cols', $escapingHeader),
            new Storage\Snowflake\Table($this->getDestinationSchemaName(), 'out.csv_2Cols'),
            $this->getSimpleImportOptions(),
            [['a', 'b'], ['c', 'd']],
            2,
        ];
        $tests[] = [
            new Storage\Snowflake\Table($this->getSourceSchemaName(), 'types', [
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
            ]),
            new Storage\Snowflake\Table(
                $this->getDestinationSchemaName(),
                'types'
            ),
            $this->getSimpleImportOptions(),
            [['a', '10.5', '0.3', 'true']],
            1,
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
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
