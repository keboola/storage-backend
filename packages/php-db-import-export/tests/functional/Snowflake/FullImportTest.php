<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Snowflake\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use PHPUnit\Framework\Attributes\DataProvider;

class FullImportTest extends SnowflakeImportExportBaseTest
{
    /**
     * @return array<mixed>
     */
    public static function fullImportData(): array
    {
        $expectedEscaping = [];
        $file = new CsvFile(self::DATA_DIR . 'escaping/standard-with-enclosures.csv');
        foreach ($file as $row) {
            $expectedEscaping[] = $row;
        }
        $escapingHeader = array_shift($expectedEscaping); // remove header

        $expectedAccounts = [];
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        foreach ($file as $row) {
            $expectedAccounts[] = $row;
        }
        $accountsHeader = array_shift($expectedAccounts); // remove header

        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();

        $file = new CsvFile(self::DATA_DIR . 'lemma.csv');
        $expectedLemma = [];
        foreach ($file as $row) {
            $expectedLemma[] = $row;
        }
        $lemmaHeader = array_shift($expectedLemma);

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        $tests = [];

        // full imports
        $tests[] = [
            static::getSourceInstance(
                'sliced/2cols-large/%MANIFEST_PREFIX%2cols-large.csvmanifest',
                $escapingHeader,
                true,
            ),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
        ];

        $tests[] = [
            static::getSourceInstance('empty.manifest', $escapingHeader, true),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
        ];

        $tests[] = [
            static::getSourceInstance('lemma.csv', $lemmaHeader),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.lemma'),
            static::getSimpleImportOptions(),
            $expectedLemma,
            5,
        ];

        $tests[] = [
            static::getSourceInstance('standard-with-enclosures.csv', $escapingHeader),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            static::getSourceInstance('gzipped-standard-with-enclosures.csv.gz', $escapingHeader),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            static::getSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingHeader,
            ),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            static::getSourceInstanceFromCsv('raw.rs.csv', new CsvOptions("\t", '', '\\'), $escapingHeader),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(),
            $expectedEscaping,
            7,
        ];

        $tests[] = [
            static::getSourceInstance('tw_accounts.changedColumnsOrder.csv', $accountChangedColumnsOrderHeader),
            new Storage\Snowflake\Table(
                static::getDestinationSchemaName(),
                'accounts-3',
            ),
            static::getSimpleImportOptions(),
            $expectedAccounts,
            3,
        ];
        $tests[] = [
            static::getSourceInstance('tw_accounts.csv', $accountsHeader),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'accounts-3'),
            static::getSimpleImportOptions(),
            $expectedAccounts,
            3,
        ];
        // manifests
        $tests[] = [
            static::getSourceInstance('sliced/accounts/%MANIFEST_PREFIX%accounts.csvmanifest', $accountsHeader, true),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'accounts-3'),
            static::getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
        ];

        $tests[] = [
            static::getSourceInstance(
                'sliced/accounts-gzip/%MANIFEST_PREFIX%accounts-gzip.csvmanifest',
                $accountsHeader,
                true,
            ),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'accounts-3'),
            static::getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
        ];

        // folder
        $tests[] = [
            static::getSourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsHeader,
                true,
                true,
            ),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'accounts-3'),
            static::getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
        ];

        // reserved words
        $tests[] = [
            static::getSourceInstance('reserved-words.csv', ['column', 'table'], false),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'table'),
            static::getSimpleImportOptions(),
            [['table', 'column']],
            1,
        ];
        // import table with _timestamp columns - used by snapshots
        $tests[] = [
            static::getSourceInstance(
                'with-ts.csv',
                [
                    'col1',
                    'col2',
                    '_timestamp',
                ],
                false,
            ),
            new Storage\Snowflake\Table(
                static::getDestinationSchemaName(),
                'out.csv_2Cols',
            ),
            static::getSimpleImportOptions(),
            [
                ['a', 'b', '2014-11-10 13:12:06.000'],
                ['c', 'd', '2014-11-10 14:12:06.000'],
            ],
            2,
        ];
        // test creating table without _timestamp column
        $tests[] = [
            static::getSourceInstance('standard-with-enclosures.csv', $escapingHeader, false),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.no_timestamp_table'),
            new ImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE,
            ),
            $expectedEscaping,
            7,
        ];
        // copy from table
        $tests[] = [
            new Storage\Snowflake\Table(static::getSourceSchemaName(), 'out.csv_2Cols', $escapingHeader),
            new Storage\Snowflake\Table(static::getDestinationSchemaName(), 'out.csv_2Cols'),
            static::getSimpleImportOptions(),
            [['a', 'b'], ['c', 'd']],
            2,
        ];
        $tests[] = [
            new Storage\Snowflake\Table(
                static::getSourceSchemaName(),
                'types',
                [
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
                ],
            ),
            new Storage\Snowflake\Table(
                static::getDestinationSchemaName(),
                'types',
            ),
            static::getSimpleImportOptions(),
            [['a', '10.5', '0.3', 'true']],
            1,
        ];

        return $tests;
    }

    /**
     * @param Storage\Snowflake\Table $destination
     * @param array<mixed> $expected
     */
    #[DataProvider('fullImportData')]
    public function testFullImport(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
    ): void {
        $result = (new Importer($this->connection))->importTable(
            $source,
            $destination,
            $options,
        );

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());
        $this->assertTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0,
        );
    }
}
