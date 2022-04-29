<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class FullImportNoTypesTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
    public function fullImportData(): \Generator
    {
        [
            $escapingHeader,
            $expectedEscaping,
        ] = $this->getExpectationFileData(
            'escaping/standard-with-enclosures.csv',
            self::EXPECTATION_FILE_DATA_KEEP_AS_IS
        );

        [
            $accountsHeader,
            $expectedAccounts,
        ] = $this->getExpectationFileData(
            'tw_accounts.csv',
            self::EXPECTATION_FILE_DATA_KEEP_AS_IS
        );

        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();

        [
            $lemmaHeader,
            $expectedLemma,
        ] = $this->getExpectationFileData(
            'lemma.csv',
            self::EXPECTATION_FILE_DATA_KEEP_AS_IS
        );

        // large sliced manifest
        $expectedLargeSlicedManifest = [];
        for ($i = 0; $i <= 1500; $i++) {
            $expectedLargeSlicedManifest[] = ['a', 'b'];
        }

        // full imports
        yield 'large manifest' => [
            $this->createABSSourceInstance(
                'sliced/2cols-large/2cols-large.csvmanifest',
                $escapingHeader,
                true,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedLargeSlicedManifest,
            1501,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'empty manifest' => [
            $this->createABSSourceInstance(
                'empty.manifest',
                $escapingHeader,
                true,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            [],
            0,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'lemma' => [
            $this->createABSSourceInstance(
                'lemma.csv',
                $lemmaHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_LEMMA],
            $this->getSynapseImportOptions(),
            $expectedLemma,
            5,
            [self::TABLE_OUT_LEMMA],
        ];

        yield 'standard with enclosures' => [
            $this->createABSSourceInstance(
                'standard-with-enclosures.csv',
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSynapseImportOptions(),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'gzipped standard with enclosure' => [
            $this->createABSSourceInstance(
                'gzipped-standard-with-enclosures.csv.gz',
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSynapseImportOptions(),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'standard with enclosures tabs' => [
            $this->createABSSourceInstanceFromCsv(
                'standard-with-enclosures.tabs.csv',
                new CsvOptions("\t"),
                $escapingHeader,
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSynapseImportOptions(),
            $expectedEscaping,
            7,
            [self::TABLE_OUT_CSV_2COLS],
        ];

        yield 'accounts changedColumnsOrder' => [
            $this->createABSSourceInstance(
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
            $this->getSynapseImportOptions(),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        yield 'accounts' => [
            $this->createABSSourceInstance(
                'tw_accounts.csv',
                $accountsHeader,
                false,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSynapseImportOptions(),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        yield 'accounts crlf' => [
            $this->createABSSourceInstance(
                'tw_accounts.crlf.csv',
                $accountsHeader,
                false,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSynapseImportOptions(),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];
        // manifests
        yield 'accounts sliced' => [
            $this->createABSSourceInstance(
                'sliced/accounts/accounts.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        yield 'accounts sliced gzip' => [
            $this->createABSSourceInstance(
                'sliced/accounts-gzip/accounts-gzip.csvmanifest',
                $accountsHeader,
                true,
                false,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        // folder
        yield 'accounts sliced folder import' => [
            $this->createABSSourceInstance(
                'sliced_accounts_no_manifest/',
                $accountsHeader,
                true,
                true,
                ['id']
            ),
            [$this->getDestinationSchemaName(), self::TABLE_ACCOUNTS_3],
            $this->getSynapseImportOptions(ImportOptions::SKIP_NO_LINE),
            $expectedAccounts,
            3,
            [self::TABLE_ACCOUNTS_3],
        ];

        // reserved words
        yield 'reserved words' => [
            $this->createABSSourceInstance(
                'reserved-words.csv',
                ['column', 'table'],
                false,
                false,
                []
            ),
            [$this->getDestinationSchemaName(), self::TABLE_TABLE],
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
                false,
                false,
                []
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_CSV_2COLS,
            ],
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
                false,
                false,
                []
            ),
            [
                $this->getDestinationSchemaName(),
                self::TABLE_OUT_NO_TIMESTAMP_TABLE,
            ],
            new SynapseImportOptions(
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
            [self::TABLE_OUT_NO_TIMESTAMP_TABLE],
        ];
        // copy from table
        yield 'copy from table' => [
            new Storage\Synapse\Table($this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS, $escapingHeader),
            [$this->getDestinationSchemaName(), self::TABLE_OUT_CSV_2COLS],
            $this->getSynapseImportOptions(
                ImportOptions::SKIP_FIRST_LINE,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE
            ),
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
           [
                $this->getDestinationSchemaName(),
                'types',
            ],
            $this->getSynapseImportOptions(
                ImportOptions::SKIP_FIRST_LINE,
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE
            ),
            [['a', '10.5', '0.3', '1']],
            1,
            [self::TABLE_TYPES],
        ];
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
        SynapseImportOptions $options,
        array $expected,
        int $expectedImportedRowCount,
        array $tablesToInit
    ): void {
        $this->initTables($tablesToInit);

        [$schemaName, $tableName] = $table;
        $destination = (new SynapseTableReflection(
            $this->connection,
            $schemaName,
            $tableName
        ))->getTableDefinition();

        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
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

        $this->assertSynapseTableEqualsExpected(
            $source,
            $destination,
            $options,
            $expected,
            0
        );
    }
}
