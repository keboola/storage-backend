<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery\ToFinal;

use Generator;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Bigquery\BigqueryBaseTestCase;

class FullImportTest extends BigqueryBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    public function testLoadToTableWithNullValuesShouldPass(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK, $this->getDestinationDbName());

        // skipping header
        $options = new BigqueryImportOptions(
            [],
            false,
            false,
            BigqueryImportOptions::SKIP_FIRST_LINE,
            BigqueryImportOptions::USING_TYPES_STRING
        );
        $source = $this->getSourceInstance(
            'multi-pk_null.csv',
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

        $importer = new ToStageImporter($this->bqClient);
        $destinationRef = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_SINGLE_PK
        );
        /** @var BigqueryTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $destination = $this->cloneDefinitionWithDedupCol($destination, ['VisitID']);
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        ));
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->bqClient);

        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $destinationRef->refresh();
        self::assertEquals(5, $destinationRef->getRowsCount());
    }

    public function testLoadToFinalTableWithoutDedup(): void
    {
        $this->initTable(self::TABLE_COLUMN_NAME_ROW_NUMBER, $this->getDestinationDbName());

        // skipping header
        $options = new BigqueryImportOptions(
            [],
            false,
            false,
            BigqueryImportOptions::SKIP_FIRST_LINE,
            BigqueryImportOptions::USING_TYPES_STRING
        );
        $source = $this->getSourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->bqClient);
        $destinationRef = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_COLUMN_NAME_ROW_NUMBER
        );
        /** @var BigqueryTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'row_number',
        ]);
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        ));
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->bqClient);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $destinationRef->refresh();
        self::assertEquals(2, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithSinglePK(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK, $this->getDestinationDbName());

        // skipping header
        $options = new BigqueryImportOptions(
            [],
            false,
            false,
            BigqueryImportOptions::SKIP_FIRST_LINE,
            BigqueryImportOptions::USING_TYPES_STRING
        );
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

        $importer = new ToStageImporter($this->bqClient);
        $destinationRef = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_SINGLE_PK
        );
        /** @var BigqueryTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $destination = $this->cloneDefinitionWithDedupCol($destination, ['VisitID']);
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        ));
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->bqClient);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $destinationRef->refresh();
        self::assertEquals(4, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithMultiPK(): void
    {
        $this->initTable(self::TABLE_MULTI_PK, $this->getDestinationDbName());

        // skipping header
        $options = new BigqueryImportOptions(
            [],
            false,
            false,
            BigqueryImportOptions::SKIP_FIRST_LINE,
            BigqueryImportOptions::USING_TYPES_STRING
        );
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

        $importer = new ToStageImporter($this->bqClient);
        $destinationRef = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_MULTI_PK
        );
        /** @var BigqueryTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $destination = $this->cloneDefinitionWithDedupCol($destination, ['VisitID', 'Something']);
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        ));
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );

        // now 6 lines. Add one with same VisitId and Something as an existing line has
        // -> expecting that this line will be skipped when DEDUP
        $this->bqClient->runQuery($this->bqClient->query(
            sprintf(
                "INSERT INTO %s.%s VALUES ('134', 'xx', 'yy', 'abc', 'def');",
                BigqueryQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                BigqueryQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        ));
        $toFinalTableImporter = new FullImporter($this->bqClient);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $destinationRef->refresh();
        self::assertEquals(6, $destinationRef->getRowsCount());
    }

    /**
     * @return Generator<string, array<mixed>>
     */
    public function fullImportData(): Generator
    {
        $escapingStub = $this->getParseCsvStub('escaping/standard-with-enclosures.csv');
        $accountsStub = $this->getParseCsvStub('tw_accounts.csv');
        $accountsChangedColumnsOrderStub = $this->getParseCsvStub('tw_accounts.changedColumnsOrder.csv');
        $lemmaStub = $this->getParseCsvStub('lemma.csv');

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        yield 'large manifest' => [
            $this->getSourceInstance(
                'sliced/2cols-large/%MANIFEST_PREFIX%2cols-large.csvmanifest',
                $escapingStub->getColumns(),
                true,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'empty manifest' => [
            $this->getSourceInstance(
                'empty.manifest',
                $escapingStub->getColumns(),
                true,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'lemma' => [
            $this->getSourceInstance(
                'lemma.csv',
                $lemmaStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_LEMMA],
            $this->getSimpleImportOptions(),
            $lemmaStub->getRows(),
            5,
            self::TABLE_OUT_LEMMA,
        ];

        yield 'standard with enclosures' => [
            $this->getSourceInstance(
                'standard-with-enclosures.csv',
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'gzipped standard with enclosure' => [
            $this->getSourceInstance(
                'gzipped-standard-with-enclosures.csv.gz',
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'standard with enclosures tabs' => [
            $this->getSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        yield 'accounts changedColumnsOrder' => [
            $this->getSourceInstance(
                'tw_accounts.changedColumnsOrder.csv',
                $accountsChangedColumnsOrderStub->getColumns(),
                false,
                false,
                ['id']
            ),
            [
                $this->getDestinationDbName(),
                self::TABLE_ACCOUNTS_3,
            ],
            $this->getSimpleImportOptions(),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];
        yield 'accounts' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // line ending detection is not supported yet for S3
        yield 'accounts crlf' => [
            $this->getSourceInstance(
                'tw_accounts.crlf.csv',
                $accountsStub->getColumns(),
                false,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        // manifests
        yield 'accounts sliced' => [
            $this->getSourceInstance(
                'sliced/accounts/%MANIFEST_PREFIX%accounts.csvmanifest',
                $accountsStub->getColumns(),
                true,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $accountsStub->getRows(),
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        yield 'accounts sliced gzip' => [
            $this->getSourceInstance(
                'sliced/accounts-gzip/%MANIFEST_PREFIX%accounts-gzip.csvmanifest',
                $accountsStub->getColumns(),
                true,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $accountsStub->getRows(),
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
                ['a', 'b', '2014-11-10 13:12:06'],
                ['c', 'd', '2014-11-10 14:12:06'],
            ],
            2,
            self::TABLE_OUT_CSV_2COLS,
        ];
        // test creating table without _timestamp column
        yield 'table without _timestamp column' => [
            $this->getSourceInstance(
                'standard-with-enclosures.csv',
                $escapingStub->getColumns(),
                false,
                false,
                []
            ),
            [
                $this->getDestinationDbName(),
                self::TABLE_OUT_NO_TIMESTAMP_TABLE,
            ],
            new BigqueryImportOptions(
                [],
                false,
                false, // don't use timestamp
                ImportOptions::SKIP_FIRST_LINE
            ),
            $escapingStub->getRows(),
            7,
            self::TABLE_OUT_NO_TIMESTAMP_TABLE,
        ];
        // copy from table
        yield 'copy from table' => [
            new Table($this->getSourceDbName(), self::TABLE_OUT_CSV_2COLS, $escapingStub->getColumns()),
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
            [['a', '10.5', '0.3', 'true']],
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
        BigqueryImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var BigqueryTableDefinition $destination */
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );

        $qb = new BigqueryTableQueryBuilder();
        $this->bqClient->runQuery($this->bqClient->query(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        ));
        $toStageImporter = new ToStageImporter($this->bqClient);
        $toFinalTableImporter = new FullImporter($this->bqClient);
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
            $sql = (new SqlBuilder())->getTableExistsCommand(
                $stagingTable->getSchemaName(),
                $stagingTable->getTableName()
            );
            $current = (array) $this->bqClient->runQuery($this->bqClient->query($sql))->getIterator()->current();
            $count = $current['count'];

            if ($count > 0) {
                $this->bqClient->runQuery($this->bqClient->query((new SqlBuilder())->getDropTableUnsafe(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName()
                )));
            }
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        $this->assertBigqueryTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
