<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\Db\ImportExport\S3SourceTrait;

class FullImportNoTypes2Test extends ExasolBaseTestCase
{
    use S3SourceTrait;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
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

        // full imports
//        yield 'large manifest' => [
//            $this->createS3SourceInstance(
//                'sliced/2cols-large/S3.2cols-large.csvmanifest',
//                $escapingHeader,
//                true,
//                false,
//                []
//            ),
//            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
//            $this->getExasolImportOptions(ImportOptions::SKIP_NO_LINE),
//            $expectedLargeSlicedManifest,
//            1501,
//            self::TABLE_OUT_CSV_2COLS,
//        ];

        yield 'empty manifest' => [
            $this->createS3SourceInstance(
                'empty.manifest',
                $escapingHeader,
                true,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getExasolImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'lemma' => [
            $this->createS3SourceInstance(
                'lemma.csv',
                $lemmaHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_LEMMA],
            $this->getExasolImportOptions(),
            $expectedLemma,
            5,
            self::TABLE_OUT_LEMMA,
        ];

        yield 'standard with enclosures' => [
            $this->createS3SourceInstance(
                'standard-with-enclosures.csv',
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getExasolImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'gzipped standard with enclosure' => [
            $this->createS3SourceInstance(
                'gzipped-standard-with-enclosures.csv.gz',
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getExasolImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'standard with enclosures tabs' => [
            $this->createS3SourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getExasolImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'accounts changedColumnsOrder' => [
            $this->createS3SourceInstance(
                'tw_accounts.changedColumnsOrder.csv',
                $accountChangedColumnsOrderHeader,
                false,
                false,
                ['id']
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_ACCOUNTS_3,
            ],
            $this->getExasolImportOptions(),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'accounts' => [
            $this->createS3SourceInstance(
                'tw_accounts.csv',
                $accountsHeader,
                false,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getExasolImportOptions(),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'accounts crlf' => [
            $this->createS3SourceInstance(
                'tw_accounts.crlf.csv',
                $accountsHeader,
                false,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getExasolImportOptions(),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];
        // manifests
        yield 'accounts sliced' => [
            $this->createS3SourceInstance(
                'sliced/accounts/accounts.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getExasolImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        yield 'accounts sliced gzip' => [
            $this->createS3SourceInstance(
                'sliced/accounts-gzip/accounts-gzip.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getExasolImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // folder
        yield 'accounts sliced folder import' => [
            $this->createS3SourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsHeader,
                true,
                true,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getExasolImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // reserved words
        yield 'reserved words' => [
            $this->createS3SourceInstance(
                'reserved-words.csv',
                ['column', 'table'],
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_TABLE],
            $this->getExasolImportOptions(),
            [['table', 'column']],
            1,
            self::TABLE_TABLE,
        ];
        // import table with _timestamp columns - used by snapshots
        yield 'import with _timestamp columns' => [
            $this->createS3SourceInstance(
                'with-ts.csv',
                [
                    'col1',
                    'col2',
                    '_timestamp',
                ],
                false,
                false,
                []
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_CSV_2COLS,
            ],
            $this->getExasolImportOptions(),
            [
                ['a', 'b', '2014-11-10 13:12:06.0000000'],
                ['c', 'd', '2014-11-10 14:12:06.0000000'],
            ],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        // test creating table without _timestamp column
        yield 'table without _timestamp column' => [
            $this->createS3SourceInstance(
                'standard-with-enclosures.csv',
                $escapingHeader,
                false,
                false,
                []
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_NO_TIMESTAMP_TABLE,
            ],
            new ExasolImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE,
                // @phpstan-ignore-next-line
                (string) getenv('CREDENTIALS_IMPORT_TYPE'),
                // @phpstan-ignore-next-line
                (string) getenv('TEMP_TABLE_TYPE')
            ),
            $expectedEscaping,
            7,
            self::TABLE_OUT_NO_TIMESTAMP_TABLE,
        ];
//        // copy from table
//        yield 'copy from table' => [
//            new Storage\Exasol\Table($this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingHeader),
//            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
//            $this->getExasolImportOptions(
//                ImportOptions::SKIP_FIRST_LINE,
//                ExasolImportOptions::DEDUP_TYPE_TMP_TABLE
//            ),
//            [['a', 'b'], ['c', 'd']],
//            2,
//            self::TABLE_OUT_CSV_2COLS],
//        ];
//        yield ' copy from table 2' => [
//            new Storage\Exasol\Table(
//                $this->getSourceSchemaName(),
//                'types',
//                [
//                    'charCol',
//                    'numCol',
//                    'floatCol',
//                    'boolCol',
//                ]
//            ),
//           [
//                $this->getDestinationSchemaName(),
//                'types',
//            ],
//            $this->getExasolImportOptions(
//                ImportOptions::SKIP_FIRST_LINE,
//                ExasolImportOptions::DEDUP_TYPE_TMP_TABLE
//            ),
//            [['a', '10.5', '0.3', '1']],
//            1,
//            self::TABLE_TYPES],
//        ];
    }

    /**
     * @dataProvider  fullImportData
     * @param array<string, string> $table
     * @param array<mixed> $expected
     * @param string[] $tablesToInit
     */
    public function testFullImport(
        Storage\SourceInterface $source,
        array $table,
        ExasolImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var ExasolTableDefinition $destination */
        $destination = (new ExasolTableReflection(
            $this->connection,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $toStageImporter = new ToStageImporter($this->connection);
        $toFinalTableImporter = new FullImporter($this->connection);
        try {
            $importState = $toStageImporter->importToStagingTable(
                $source,
                $stagingTable,
                $options
            );
            $result = $toFinalTableImporter->importToTable(
                $stagingTable,
                $destination,
                $options,
                $importState
            );
        } finally {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName()
                )
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertExasolTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
