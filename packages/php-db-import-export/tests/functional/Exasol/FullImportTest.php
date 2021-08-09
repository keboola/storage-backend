<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Exasol\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\Db\ImportExport\S3SourceTrait;

class FullImportTest extends ExasolBaseTestCase
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

    public function testLoadToFinalTableWitoutDedup(): void
    {
        $this->initTable(self::TABLE_COLUMN_NAME_ROW_NUMBER);

        // skipping header
        $options = $this->getExasolImportOptions(1, false);
        $source = $this->createS3SourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_COLUMN_NAME_ROW_NUMBER
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'row_number',
        ]);
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(2, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithSinglePK(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK);

        // skipping header
        $options = $this->getExasolImportOptions(1, false);
        $source = $this->createS3SourceInstance(
            'multi-pk.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_SINGLE_PK
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(4, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithMultiPK(): void
    {
        $this->initTable(self::TABLE_MULTI_PK);

        // skipping header
        $options = $this->getExasolImportOptions(1, false);
        $source = $this->createS3SourceInstance(
            'multi-pk.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID', 'Something']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_MULTI_PK
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );

        // now 6 lines. Add one with same VisitId and Something as an existing line has
        // -> expecting that this line will be skipped when DEDUP
        $this->connection->executeQuery(
            sprintf(
                "INSERT INTO %s.%s VALUES ('134', 'xx', 'yy', 'abc', 'def');",
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(6, $destinationRef->getRowsCount());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public function fullImportData(): Generator
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

        yield 'large manifest' => [
            $this->createS3SourceInstance(
                'sliced/2cols-large/S3.2cols-large.csvmanifest',
                $escapingHeader,
                true,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getExasolImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            self::TABLE_OUT_CSV_2COLS,
        ];

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
                'sliced/accounts/S3.accounts.csvmanifest',
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
                'sliced/accounts-gzip/S3.accounts-gzip.csvmanifest',
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
                ['a', 'b', '2014-11-10 13:12:06.000000'],
                ['c', 'd', '2014-11-10 14:12:06.000000'],
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
                ImportOptions::SKIP_FIRST_LINE
            ),
            $expectedEscaping,
            7,
            self::TABLE_OUT_NO_TIMESTAMP_TABLE,
        ];
        // copy from table
        yield 'copy from table' => [
            new Table($this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingHeader),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getExasolImportOptions(),
            [['a', 'b'], ['c', 'd']],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        yield 'copy from table 2' => [
            new Table(
                $this->getSourceSchemaName(),
                self::TABLE_TYPES,
                [
                    'charCol',
                    'numCol',
                    'floatCol',
                    'boolCol',
                ]
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_TYPES,
            ],
            $this->getExasolImportOptions(),
            [['a', '10.5', '0.3', '1']],
            1,
            self::TABLE_TYPES,
        ];
    }

    /**
     * @dataProvider  fullImportData
     * @param array<string, string> $table
     * @param array<mixed> $expected
     * @param string $tablesToInit
     */
    public function testFullImportWithDataSet(
        SourceInterface $source,
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
