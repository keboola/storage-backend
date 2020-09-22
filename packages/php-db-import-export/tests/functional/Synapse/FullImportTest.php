<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Synapse\Importer;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;

class FullImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function fullImportData(): \Generator
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
        yield 'large manifest' => [
            $this->createABSSourceInstance('sliced/2cols-large/2cols-large.csvmanifest', $escapingHeader, true),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'empty manifest' => [
            $this->createABSSourceInstance('empty.manifest', $escapingHeader, true),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'lemma' => [
            $this->createABSSourceInstance('lemma.csv', $lemmaHeader),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_LEMMA),
            $this->getSynapseImportOptions(),
            $expectedLemma,
            5,
            [self::TABLE_OUT_LEMMA],
        ];

        yield 'standard with enclosures' => [
            $this->createABSSourceInstance('standard-with-enclosures.csv', $escapingHeader),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
            $this->getSynapseImportOptions(),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'gzipped standard with enclosure' => [
            $this->createABSSourceInstance('gzipped-standard-with-enclosures.csv.gz', $escapingHeader),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
            $this->getSynapseImportOptions(),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'standard with enclosures tabs' => [
            $this->createABSSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingHeader
            ),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
            $this->getSynapseImportOptions(),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

//        yield 'x = [>
//            $this->createABSSourceInstanceFromCsv('raw.rs.csv', new CsvOptions("\t", '', '\\')),
//            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
//            $this->getSynapseImportOptions($escapingHeader),
//            $expectedEscaping,
//            7,
//            [self::TABLE_OUT_CSV_2COLS],
//        ];

        yield 'accounts changedColumnsOrder' => [
            $this->createABSSourceInstance(
                'tw_accounts.changedColumnsOrder.csv',
                $accountChangedColumnsOrderHeader
            ),
            new Storage\Synapse\Table(
                $this->getDestinationSchemaName(),
                self::TABLE_ACCOUNTS_3
            ),
            $this->getSynapseImportOptions(),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        yield 'accounts' => [
            $this->createABSSourceInstance('tw_accounts.csv', $accountsHeader),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3),
            $this->getSynapseImportOptions(),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        // manifests
        yield 'accounts sliced' => [
            $this->createABSSourceInstance('sliced/accounts/accounts.csvmanifest', $accountsHeader, true),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3),
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        yield 'accounts sliced gzip' => [
            $this->createABSSourceInstance('sliced/accounts-gzip/accounts-gzip.csvmanifest', $accountsHeader, true),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3),
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        // reserved words
        yield 'reserved words' => [
            $this->createABSSourceInstance('reserved-words.csv', ['column', 'table'], false),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_TABLE),
            $this->getSynapseImportOptions(),
            [['table', 'column']],
            1,
            [self::TABLE_TABLE],
        ];
        // import table with _timestamp columns - used by snapshots
        yield 'import with _timestamp columns' => [
            $this->createABSSourceInstance(
                'with-ts.csv',
                [
                    'col1',
                    'col2',
                    '_timestamp',
                ],
                false
            ),
            new Storage\Synapse\Table(
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_CSV_2COLS
            ),
            $this->getSynapseImportOptions(),
            [
                ['a', 'b', '2014-11-10 13:12:06.0000000'],
                ['c', 'd', '2014-11-10 14:12:06.0000000'],
            ],
            2,
            [self::TABLE_OUT_CSV_2COLS],
        ];
        // test creating table without _timestamp column
        yield 'table without _timestamp column' => [
            $this->createABSSourceInstance(
                'standard-with-enclosures.csv',
                $escapingHeader,
                false
            ),
            new Storage\Synapse\Table(
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_NO_TIMESTAMP_TABLE
            ),
            new SynapseImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE,
                getenv('CREDENTIALS_IMPORT_TYPE')
            ),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_NO_TIMESTAMP_TABLE],
        ];
        // copy from table
        yield 'copy from table' => [
            new Storage\Synapse\Table($this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingHeader),
            new Storage\Synapse\Table($this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS),
            $this->getSynapseImportOptions(),
            [['a', 'b'], ['c', 'd']],
            2,
            [self::TABLE_OUT_CSV_2COLS],
        ];
        yield ' copy from table 2' => [
            new Storage\Synapse\Table(
                $this->getSourceSchemaName(),
                'types',
                [
                    'charCol',
                    'numCol',
                    'floatCol',
                    'boolCol',
                ]
            ),
            new Storage\Synapse\Table(
                $this->getDestinationSchemaName(),
                'types'
            ),
            $this->getSynapseImportOptions(),
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
        SynapseImportOptions $options,
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
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
