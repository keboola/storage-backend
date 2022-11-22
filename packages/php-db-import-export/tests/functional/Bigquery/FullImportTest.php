<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Generator;
use Google\Cloud\BigQuery\Timestamp;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
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

    public function testLoadToFinalTableWithoutDedup(): void
    {
        // table translations checks numeric and string-ish data
        $this->initTable(self::TABLE_TRANSLATIONS);

        // skipping header
        $options = $this->getImportOptions([], false, false, 1);
        $source = $this->createGCSSourceInstance(
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

        $importer = new ToStageImporter($this->bqClient);
        $destinationRef = new BigqueryTableReflection(
            $this->bqClient,
            $this->getDestinationDbName(),
            self::TABLE_TRANSLATIONS
        );
        /** @var BigqueryTableDefinition $destination */
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'name',
            'price',
            'isDeleted',
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

        self::assertEquals(3, $destinationRef->getRowsCount());
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
                'sliced/2cols-large/GCS.2cols-large.csvmanifest',
                $escapingHeader,
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
                'sliced/accounts/GCS.accounts.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            self::TABLE_ACCOUNTS_3,
        ];

        yield 'accounts sliced gzip' => [
            $this->getSourceInstance(
                'sliced/accounts-gzip/GCS.accounts-gzip.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationDbName(), self::TABLE_ACCOUNTS_3],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
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
                ['a', 'b', '2014-11-10 13:12:06.000000+00:00'],
                ['c', 'd', '2014-11-10 14:12:06.000000+00:00'],
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
                ImportOptions::SKIP_FIRST_LINE
            ),
            $expectedEscaping,
            7,
            self::TABLE_OUT_NO_TIMESTAMP_TABLE,
        ];
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
        ImportOptions $options,
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

    /**
     * @param int|string $sortKey
     * @param array<mixed> $expected
     * @param string|int $sortKey
     */
    protected function assertBigqueryTableEqualsExpected(
        SourceInterface $source,
        BigqueryTableDefinition $destination,
        ImportOptions $options,
        array $expected,
        $sortKey,
        string $message = 'Imported tables are not the same as expected'
    ): void {
        $tableColumns = (new BigqueryTableReflection(
            $this->bqClient,
            $destination->getSchemaName(),
            $destination->getTableName()
        ))->getColumnsNames();

        if ($options->useTimestamp()) {
            self::assertContains('_timestamp', $tableColumns);
        } else {
            self::assertNotContains('_timestamp', $tableColumns);
        }

        if (!in_array('_timestamp', $source->getColumnsNames(), true)) {
            $tableColumns = array_filter($tableColumns, static function ($column) {
                return $column !== '_timestamp';
            });
        }

        $tableColumns = array_map(static function ($column) {
            return sprintf('%s', $column);
        }, $tableColumns);

        $sql = sprintf(
            'SELECT %s FROM %s.%s',
            implode(', ', array_map(static function ($item) {
                return BigqueryQuote::quoteSingleIdentifier($item);
            }, $tableColumns)),
            BigqueryQuote::quoteSingleIdentifier($destination->getSchemaName()),
            BigqueryQuote::quoteSingleIdentifier($destination->getTableName())
        );

        $queryResult = array_map(static function (array $row) {
            return array_map(static function ($column) {
                if ($column instanceof Timestamp) {
                    return $column->formatAsString();
                }
                return $column;
            }, array_values($row));
        }, iterator_to_array($this->bqClient->runQuery($this->bqClient->query($sql))->getIterator()));

        $this->assertArrayEqualsSorted(
            $expected,
            $queryResult,
            $sortKey,
            $message
        );
    }
}
