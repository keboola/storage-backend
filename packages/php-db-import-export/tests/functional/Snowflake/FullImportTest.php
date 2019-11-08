<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\SourceStorage;

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
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader, ImportOptions::SKIP_NO_LINE),
            $this->createABSSourceInstance('sliced/2cols-large/2cols-large.csvmanifest', true),
            $expectedLargeSlicedManifest,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader, ImportOptions::SKIP_NO_LINE),
            $this->createABSSourceInstance('empty.manifest', true),
            [],
        ];

        $tests[] = [
            $this->getSimpleImportOptions('out.lemma', $lemmaHeader),
            $this->createABSSourceInstance('lemma.csv'),
            $expectedLemma,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader),
            $this->createABSSourceInstance('standard-with-enclosures.csv'),
            $expectedEscaping,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader),
            $this->createABSSourceInstance('gzipped-standard-with-enclosures.csv.gz'),
            $expectedEscaping,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader),
            $this->createABSSourceInstanceFromCsv(
                new CsvFile(self::DATA_DIR . 'standard-with-enclosures.tabs.csv', "\t")
            ),
            $expectedEscaping,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader),
            $this->createABSSourceInstanceFromCsv(new CsvFile(self::DATA_DIR . 'raw.rs.csv', "\t", '', '\\')),
            $expectedEscaping,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('accounts-3', $accountChangedColumnsOrderHeader),
            $this->createABSSourceInstance('tw_accounts.changedColumnsOrder.csv'),
            $expectedAccounts,
        ];
        $tests[] = [
            $this->getSimpleImportOptions('accounts-3', $accountsHeader),
            $this->createABSSourceInstance('tw_accounts.csv'),
            $expectedAccounts,
        ];
        // manifests
        $tests[] = [
            $this->getSimpleImportOptions('accounts-3', $accountsHeader, ImportOptions::SKIP_NO_LINE),
            $this->createABSSourceInstance('sliced/accounts/accounts.csvmanifest', true),
            $expectedAccounts,
        ];

        $tests[] = [
            $this->getSimpleImportOptions('accounts-3', $accountsHeader, ImportOptions::SKIP_NO_LINE),
            $this->createABSSourceInstance('sliced/accounts-gzip/accounts-gzip.csvmanifest', true),
            $expectedAccounts,
        ];

        // reserved words
        $tests[] = [
            $this->getSimpleImportOptions('table', ['column', 'table']),
            $this->createABSSourceInstance('reserved-words.csv', false),
            [['table', 'column']],
        ];
        // import table with _timestamp columns - used by snapshots
        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', [
                'col1',
                'col2',
                '_timestamp',
            ]),
            $this->createABSSourceInstance('with-ts.csv', false),
            [
                ['a', 'b', '2014-11-10 13:12:06.000'],
                ['c', 'd', '2014-11-10 14:12:06.000'],
            ],
        ];
        // test creating table without _timestamp column
        $tests[] = [
            new ImportOptions(
                self::SNOWFLAKE_DEST_SCHEMA_NAME,
                'out.no_timestamp_table',
                [],
                $escapingHeader,
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $this->createABSSourceInstance('standard-with-enclosures.csv', false),
            $expectedEscaping,
        ];
        // copy from table
        $tests[] = [
            $this->getSimpleImportOptions('out.csv_2Cols', $escapingHeader),
            new SourceStorage\Snowflake\Source(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'out.csv_2Cols'),
            [['a', 'b'], ['c', 'd']],
        ];
        $tests[] = [
            $this->getSimpleImportOptions('types', [
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
            ]),
            new SourceStorage\Snowflake\Source(self::SNOWFLAKE_SOURCE_SCHEMA_NAME, 'types'),
            [['a', '10.5', '0.3', 'true']],
        ];

        return $tests;
    }

    /**
     * @dataProvider  fullImportData
     * @param int|string $sortKey
     */
    public function testFullImport(
        ImportOptions $options,
        SourceStorage\SourceInterface $source,
        array $expected = [],
        $sortKey = 0
    ): void {
        (new Importer($this->connection))->importTable(
            $options,
            $source
        );

        $this->assertTableEqualsExpected(
            $options,
            $expected,
            $sortKey
        );
    }
}
