<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

/**
 * Destination tables has defined types so staging table has also types
 */
class FullImportWithTypesTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    protected function initTable(string $tableName): void
    {
        $tableDistribution = 'ROUND_ROBIN';
        if (getenv('TABLE_DISTRIBUTION') !== false) {
            $tableDistribution = getenv('TABLE_DISTRIBUTION');
        }

        switch ($tableName) {
            case self::TABLE_ACCOUNTS_3:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[accounts-3] (
                [id] INT NOT NULL,
                [idTwitter] BIGINT NOT NULL,
                [name] nvarchar(4000) NOT NULL,
                [import] INT,
                [isImported] TINYINT NOT NULL,
                [apiLimitExceededDatetime] DATETIME2 NOT NULL,
                [analyzeSentiment] TINYINT NOT NULL,
                [importKloutScore] INT NOT NULL,
                [timestamp] DATETIME2 NOT NULL,
                [oauthToken] nvarchar(4000) NOT NULL,
                [oauthSecret] nvarchar(4000) NOT NULL,
                [idApp] INT NOT NULL,
                [_timestamp] datetime2,
                PRIMARY KEY NONCLUSTERED("id") NOT ENFORCED
            ) WITH (DISTRIBUTION=%s)',
                    $this->getDestinationSchemaName(),
                    $tableDistribution === 'HASH' ? 'HASH([id])' : $tableDistribution
                ));
                break;
            case self::TABLE_TYPES:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[types] (
              [charCol]  nvarchar(4000) NOT NULL,
              [numCol] decimal(10,1) NOT NULL,
              [floatCol] real NOT NULL,
              [boolCol] tinyint NOT NULL,
              [_timestamp] datetime2
            );',
                    $this->getDestinationSchemaName()
                ));

                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[types] (
              [charCol]  nvarchar(4000) NOT NULL,
              [numCol] decimal(10,1) NOT NULL,
              [floatCol] real NOT NULL,
              [boolCol] tinyint NOT NULL
            );',
                    $this->getSourceSchemaName()
                ));
                $this->connection->exec(sprintf(
                    'INSERT INTO [%s].[types] VALUES
              (\'a\', \'10.5\', \'1.4\', 1)
           ;',
                    $this->getSourceSchemaName()
                ));
                break;
        }
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
    public function fullImportData(): Generator
    {
        [
            $accountsHeader,
            $expectedAccounts,
        ] = $this->getExpectationFileData(
            'expectation.tw_accounts-typed.csv',
            self::EXPECTATION_FILE_DATA_CONVERT_NULLS
        );

        $file = new CsvFile(self::DATA_DIR . 'tw_accounts.changedColumnsOrder.csv');
        $accountChangedColumnsOrderHeader = $file->getHeader();

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

        // copy from table
        yield ' copy from table' => [
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
                SynapseImportOptions::DEDUP_TYPE_TMP_TABLE,
                SynapseImportOptions::TABLE_TYPES_CAST,
                SynapseImportOptions::SAME_TABLES_REQUIRED
            ),
            [['a', '10.5', '1.4', '1']],
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
