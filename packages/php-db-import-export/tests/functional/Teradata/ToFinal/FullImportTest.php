<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata\ToFinal;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Teradata\Table;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Teradata\TeradataBaseTestCase;

class FullImportTest extends TeradataBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    protected function tearDown(): void
    {
        $this->connection->close();
        parent::tearDown();
    }

    public function testLoadToFinalTableWithoutDedup(): void
    {
        // table translations checks numeric and string-ish data
        $this->initTable(self::TABLE_TRANSLATIONS);

        // skipping header
        $options = $this->getImportOptions([], false, false, ImportOptionsInterface::SKIP_FIRST_LINE);
        $source = $this->getSourceInstance(
            self::TABLE_TRANSLATIONS . '.csv',
            [
                'id',
                'name',
                'price',
                'isDeleted',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new TeradataTableReflection(
            $this->connection,
            $this->getDestinationDbName(),
            self::TABLE_TRANSLATIONS
        );
        /** @var TeradataTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'name',
            'price',
            'isDeleted',
        ]);
        $qb = new TeradataTableQueryBuilder();
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

        self::assertEquals(3, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithSinglePK(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK);

        // skipping header
        $options = $this->getImportOptions([], false, false, ImportOptionsInterface::SKIP_FIRST_LINE);
        $source = $this->getSourceInstance(
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
        $destinationRef = new TeradataTableReflection(
            $this->connection,
            $this->getDestinationDbName(),
            self::TABLE_SINGLE_PK
        );
        /** @var TeradataTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new TeradataTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->importToTable(
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
        $options = $this->getImportOptions([], false, false, ImportOptionsInterface::SKIP_FIRST_LINE);
        $source = $this->getSourceInstance(
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
        $destinationRef = new TeradataTableReflection(
            $this->connection,
            $this->getDestinationDbName(),
            self::TABLE_MULTI_PK
        );
        /** @var TeradataTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new TeradataTableQueryBuilder();
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
                TeradataQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                TeradataQuote::quoteSingleIdentifier($stagingTable->getTableName())
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
        /** @var string[] $escapingHeader */
        $escapingHeader = array_shift($expectedEscaping); // remove header
        $expectedEscaping = array_values($expectedEscaping);

        $expectedAccounts = [];
        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        foreach ($file as $row) {
            $expectedAccounts[] = $row;
        }
        /** @var string[] $accountsHeader */
        $accountsHeader = array_shift($expectedAccounts); // remove header
        $expectedAccounts = array_values($expectedAccounts);

        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();

        $file = new CsvFile(self::DATA_DIR . 'lemma.csv');
        $expectedLemma = [];
        foreach ($file as $row) {
            $expectedLemma[] = $row;
        }
        /** @var string[] $lemmaHeader */
        $lemmaHeader = array_shift($expectedLemma);
        $expectedLemma = array_values($expectedLemma);

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        yield 'large manifest' => [
            $this->getSourceInstance(
                'sliced/2cols-large/%MANIFEST_PREFIX%2cols-large.csvmanifest',
                $escapingHeader,
                true,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(ImportOptionsInterface::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'empty manifest' => [
            $this->getSourceInstance(
                'empty.manifest',
                $escapingHeader,
                true,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            [],
            0,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'lemma' => [
            $this->getSourceInstance(
                'lemma.csv',
                $lemmaHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_LEMMA],
            $this->getSimpleImportOptions(),
            $expectedLemma,
            5,
            self::TABLE_OUT_LEMMA,
        ];

        yield 'standard with enclosures' => [
            $this->getSourceInstance(
                'standard-with-enclosures.csv',
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'gzipped standard with enclosure' => [
            $this->getSourceInstance(
                'gzipped-standard-with-enclosures.csv.gz',
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'standard with enclosures tabs' => [
            $this->getSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'accounts changedColumnsOrder' => [
            $this->getSourceInstance(
                'tw_accounts.changedColumnsOrder.csv',
                $accountChangedColumnsOrderHeader,
                false,
                false,
                ['id']
            ),
            [
                $this->getDestinationDbName(),
                self::TABLE_ACCOUNTS_3,
            ],
            $this->getSimpleImportOptions(),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        yield 'accounts' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsHeader,
                false,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // line ending detection is not supported yet for S3
        yield 'accounts crlf' => [
            $this->getSourceInstance(
                'tw_accounts.crlf.csv',
                $accountsHeader,
                false,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // manifests
        yield 'accounts sliced' => [
            $this->getSourceInstance(
                'sliced/accounts/%MANIFEST_PREFIX%accounts.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptionsInterface::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        yield 'accounts sliced gzip' => [
            $this->getSourceInstance(
                'sliced/accounts-gzip/%MANIFEST_PREFIX%accounts-gzip.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptionsInterface::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // folder
        yield 'accounts sliced folder import' => [
            $this->getSourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsHeader,
                true,
                true,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptionsInterface::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // reserved words
        yield 'reserved words' => [
            $this->getSourceInstance(
                'reserved-words.csv',
                ['column', 'table'],
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_TABLE],
            $this->getSimpleImportOptions(),
            [['table', 'column', null]],
            1,
            self::TABLE_TABLE,
        ];
        // import table with _timestamp columns - used by snapshots
        yield 'import with _timestamp columns' => [
            $this->getSourceInstance(
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
                $this->getDestinationDbName(),
                self::TABLE_OUT_CSV_2COLS,
            ],
            $this->getSimpleImportOptions(),
            [
                ['a', 'b', '2014-11-10 13:12:06.000000'],
                ['c', 'd', '2014-11-10 14:12:06.000000'],
            ],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        // test creating table without _timestamp column
        yield 'table without _timestamp column' => [
            $this->getSourceInstance(
                'standard-with-enclosures.csv',
                $escapingHeader,
                false,
                false,
                []
            ),
            [
                $this->getDestinationDbName(),
                self::TABLE_OUT_NO_TIMESTAMP_TABLE,
            ],
            $this->getImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptionsInterface::SKIP_FIRST_LINE
            ),
            $expectedEscaping,
            7,
            self::TABLE_OUT_NO_TIMESTAMP_TABLE,
        ];
        // copy from table
        yield 'copy from table' => [
            new Table($this->getSourceDbName(), self::TABLE_OUT_CSV_2COLS, $escapingHeader),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            [['a', 'b'], ['c', 'd']],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        yield 'copy from table 2' => [
            new Table(
                $this->getSourceDbName(),
                self::TABLE_TYPES,
                [
                    'charCol',
                    'numCol',
                    'floatCol',
                    'boolCol',
                ]
            ),
            [
                $this->getDestinationDbName(),
                self::TABLE_TYPES,
            ],
            $this->getSimpleImportOptions(),
            // TODO https://keboola.atlassian.net/browse/KBC-2526 it should cast 0.3 to "0.3"
            [['a', '        10.5', ' 3.00000000000000E-001', '   1']],
            1,
            self::TABLE_TYPES,
        ];
    }

    /**
     * @dataProvider  fullImportData
     * @param array{0:string, 1:string} $table
     * @param array<mixed> $expected
     */
    public function testFullImportWithDataSet(
        SourceInterface $source,
        array $table,
        TeradataImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        if (getenv('STORAGE_TYPE') === 'ABS' && $this->dataName() === 'large manifest') {
            $this->markTestIncomplete('large manifest end with timeout');
        }

        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var TeradataTableDefinition $destination */
        $destination = (new TeradataTableReflection(
            $this->connection,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        // TPT
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );

        $qb = new TeradataTableQueryBuilder();
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
            $this->dropTableIfExists($stagingTable->getSchemaName(), $stagingTable->getTableName());
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertTeradataTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
