<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

class FullImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::SYNAPSE_DEST_SCHEMA_NAME);
        $this->dropAllWithinSchema(self::SYNAPSE_SOURCE_SCHEMA_NAME);
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', self::SYNAPSE_DEST_SCHEMA_NAME));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', self::SYNAPSE_SOURCE_SCHEMA_NAME));
    }

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
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader, ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        $tests[] = [
            $this->createABSSourceInstance('empty.manifest', true),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader, ImportOptions::SKIP_NO_LINE),
            [],
            0,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        $tests[] = [
            $this->createABSSourceInstance('lemma.csv'),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_LEMMA),
            $this->getSimpleImportOptions($lemmaHeader),
            $expectedLemma,
            5,
            [self::TABLE_OUT_LEMMA],
        ];

        $tests[] = [
            $this->createABSSourceInstance('standard-with-enclosures.csv'),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        $tests[] = [
            $this->createABSSourceInstance('gzipped-standard-with-enclosures.csv.gz'),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        $tests[] = [
            $this->createABSSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t")
            ),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

//        $tests[] = [
//            $this->createABSSourceInstanceFromCsv('raw.rs.csv', new CsvOptions("\t", '', '\\')),
//            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
//            $this->getSimpleImportOptions($escapingHeader),
//            $expectedEscaping,
//            7,
//            [self::TABLE_OUT_CSV_2COLS],
//        ];

        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.changedColumnsOrder.csv'),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_ACCOUNTS_3),
            $this->getSimpleImportOptions($accountChangedColumnsOrderHeader),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        $tests[] = [
            $this->createABSSourceInstance('tw_accounts.csv'),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_ACCOUNTS_3),
            $this->getSimpleImportOptions($accountsHeader),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        // manifests
        $tests[] = [
            $this->createABSSourceInstance('sliced/accounts/accounts.csvmanifest', true),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_ACCOUNTS_3),
            $this->getSimpleImportOptions($accountsHeader, ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        $tests[] = [
            $this->createABSSourceInstance('sliced/accounts-gzip/accounts-gzip.csvmanifest', true),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_ACCOUNTS_3),
            $this->getSimpleImportOptions($accountsHeader, ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        // reserved words
        $tests[] = [
            $this->createABSSourceInstance('reserved-words.csv', false),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_TABLE),
            $this->getSimpleImportOptions(['column', 'table']),
            [['table', 'column']],
            1,
            [self::TABLE_TABLE],
        ];
        // import table with _timestamp columns - used by snapshots
        $tests[] = [
            $this->createABSSourceInstance('with-ts.csv', false),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions([
                'col1',
                'col2',
                '_timestamp',
            ]),
            [
                ['a', 'b', '2014-11-10 13:12:06.0000000'],
                ['c', 'd', '2014-11-10 14:12:06.0000000'],
            ],
            2,
            [self::TABLE_OUT_CSV_2COLS],
        ];
        // test creating table without _timestamp column
        $tests[] = [
            $this->createABSSourceInstance('standard-with-enclosures.csv', false),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_NO_TIMESTAMP_TABLE),
            new ImportOptions(
                [],
                $escapingHeader,
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_NO_TIMESTAMP_TABLE],
        ];
        // copy from table
        $tests[] = [
            new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, self::TABLE_OUT_CSV_2COLS),
            $this->getSimpleImportOptions($escapingHeader),
            [['a', 'b'], ['c', 'd']],
            2,
            [self::TABLE_OUT_CSV_2COLS],
        ];
        $tests[] = [
            new Storage\Synapse\Table(self::SYNAPSE_SOURCE_SCHEMA_NAME, 'types'),
            new Storage\Synapse\Table(self::SYNAPSE_DEST_SCHEMA_NAME, 'types'),
            $this->getSimpleImportOptions([
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
            ]),
            [['a', '10.5', '0.3', '1']],
            1,
            [self::TABLE_TYPES],
        ];

        return $tests;
    }

    /**
     * @dataProvider  fullImportData
     * @param Storage\Synapse\Table $destination
     */
    public function testFullImport(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        array $tablesToInit
    ): void {
        $this->initTables($tablesToInit);

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
