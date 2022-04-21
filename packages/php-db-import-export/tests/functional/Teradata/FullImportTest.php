<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\Teradata\Table;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportCommon\S3SourceTrait;

class FullImportTest extends TeradataBaseTestCase
{
    use S3SourceTrait;

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
        $options = $this->getImportOptions([], false, false, 1);
        $source = $this->createS3SourceInstance(
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
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(ImportOptions::SKIP_NO_LINE),
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
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
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
            [$this->getDestinationDbName(), self::TABLE_OUT_LEMMA],
            $this->getSimpleImportOptions(),
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
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
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
            [$this->getDestinationDbName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSimpleImportOptions(),
            $expectedEscaping,
            7,
            self::TABLE_OUT_CSV_2COLS,
        ];

        if ($this->getCsvAdapter() !== TeradataImportOptions::CSV_ADAPTER_SPT) {
            // ignore this use case STP doesn't support custom delimiter
            yield 'standard with enclosures tabs' => [
                $this->createS3SourceInstanceFromCsv(
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
        }

        yield 'accounts changedColumnsOrder' => [
            $this->createS3SourceInstance(
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
            $this->createS3SourceInstance(
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
            $this->createS3SourceInstance(
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
            $this->createS3SourceInstance(
                'sliced/accounts/S3.accounts.csvmanifest',
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
            $this->createS3SourceInstance(
                'sliced/accounts-gzip/S3.accounts-gzip.csvmanifest',
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

        // folder
        yield 'accounts sliced folder import' => [
            $this->createS3SourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsHeader,
                true,
                true,
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
            $this->createS3SourceInstance(
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
            $this->createS3SourceInstance(
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
            [['a', '10.5', '3.00000000000000E-001', '1']],
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
        TeradataImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var TeradataTableDefinition $destination */
        $destination = (new TeradataTableReflection(
            $this->connection,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        if ($this->getCsvAdapter() === TeradataImportOptions::CSV_ADAPTER_TPT) {
            // TPT
            $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
                $destination,
                $source->getColumnsNames()
            );
        } else {
            // SPT
            $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithText(
                $destination,
                $source->getColumnsNames()
            );
        }

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
            if ($this->connection->fetchOne(
                (new SqlBuilder())->getTableExistsCommand(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName()
                )
            ) > 0) {
                $this->connection->executeStatement((new SqlBuilder())->getDropTableUnsafe(
                    $stagingTable->getSchemaName(),
                    $stagingTable->getTableName()
                ));
            }
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
