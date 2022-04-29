<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class IncrementalImportWithTypesTest extends SynapseBaseTestCase
{
    private const TABLE_SIMPLE = 'simple';
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
            case self::TABLE_SIMPLE:
                $this->connection->executeStatement(sprintf(
                    'CREATE TABLE [%s].[simple] (
                [Col1] INT NOT NULL,
                [Col2] NUMERIC(10,1) NOT NULL,
                [Col3] datetime2,
                PRIMARY KEY NONCLUSTERED("Col1") NOT ENFORCED
            ) WITH (DISTRIBUTION=%s)',
                    $this->getDestinationSchemaName(),
                    $tableDistribution === 'HASH' ? 'HASH([Col1])' : $tableDistribution
                ));
                break;
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
            case self::TABLE_MULTI_PK:
                $this->connection->exec(sprintf(
                    'CREATE TABLE [%s].[multi-pk] (
            [VisitID] BIGINT NOT NULL DEFAULT \'\',
            [Value] nvarchar(4000) NOT NULL DEFAULT \'\',
            [MenuItem] nvarchar(4000) NOT NULL DEFAULT \'\',
            [Something] nvarchar(4000) NOT NULL DEFAULT \'\',
            [Other] nvarchar(4000) NOT NULL DEFAULT \'\',
            [_timestamp] datetime2,
            PRIMARY KEY NONCLUSTERED("VisitID","Value","MenuItem") NOT ENFORCED
            );',
                    $this->getDestinationSchemaName()
                ));
                break;
        }
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
    public function incrementalImportData(): \Generator
    {
        [
            $accountColumns,
            $expectedAccountsRows,
        ] = $this->getExpectationFileData(
            'expectation.tw_accounts.increment-typed.csv',
            self::EXPECTATION_FILE_DATA_CONVERT_NULLS
        );

        [
            $multiPkColumns,
        ] = $this->getExpectationFileData(
            'expectation.multi-pk.increment.csv',
            self::EXPECTATION_FILE_DATA_CONVERT_NULLS
        );

        [
            $simpleColumns,
            $expectedSimpleRows,
        ] = $this->getExpectationFileData(
            'expectation.simple.increment.csv',
            self::EXPECTATION_FILE_DATA_CONVERT_NULLS
        );

        $tests = [];
        yield 'simple accounts' => [
            $this->createABSSourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            $this->getSynapseIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'accounts-3'],
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_3],
        ];
        yield 'simple types' => [
            $this->createABSSourceInstance(
                'simple.csv',
                $simpleColumns,
                false,
                false,
                ['Col1']
            ),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance(
                'simple.increment.csv',
                $simpleColumns,
                false,
                false,
                ['Col1']
            ),
            $this->getSynapseIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), self::TABLE_SIMPLE],
            $expectedSimpleRows,
            10,
            [self::TABLE_SIMPLE],
        ];
        yield 'multi pk' => [
            $this->createABSSourceInstance(
                'multi-pk.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSynapseImportOptions(),
            $this->createABSSourceInstance(
                'multi-pk.increment.csv',
                $multiPkColumns,
                false,
                false,
                ['VisitID', 'Value', 'MenuItem']
            ),
            $this->getSynapseIncrementalImportOptions(),
            [$this->getDestinationSchemaName(), 'multi-pk'],
            // Synapse is not comparing empty strings and null same as other backends
            // this can't be tested as dedup in full import is not deterministic, so we test only expected row count
            6,
            3,
            [self::TABLE_MULTI_PK],
        ];
        return $tests;
    }

    /**
     * @dataProvider  incrementalImportData
     * @param array<string,string> $table
     * @param array<mixed>|int $expected
     * @param string[] $tablesToInit
     */
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        SynapseImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        SynapseImportOptions $incrementalOptions,
        array $table,
        $expected,
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

        $toStageImporter = new ToStageImporter($this->connection);
        $fullImporter = new FullImporter($this->connection);
        $incrementalImporter = new IncrementalImporter($this->connection);

        $fullLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $fullLoadSource->getColumnsNames()
        );
        $incrementalLoadStagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $incrementalSource->getColumnsNames()
        );

        try {
            // full load
            $qb = new SynapseTableQueryBuilder();
            $this->connection->executeStatement(
                $qb->getCreateTableCommandFromDefinition($fullLoadStagingTable)
            );

            $importState = $toStageImporter->importToStagingTable(
                $fullLoadSource,
                $fullLoadStagingTable,
                $fullLoadOptions
            );
            $fullImporter->importToTable(
                $fullLoadStagingTable,
                $destination,
                $fullLoadOptions,
                $importState
            );
            // incremental load
            $qb = new SynapseTableQueryBuilder();
            $this->connection->executeStatement(
                $qb->getCreateTableCommandFromDefinition($incrementalLoadStagingTable)
            );
            $importState = $toStageImporter->importToStagingTable(
                $incrementalSource,
                $incrementalLoadStagingTable,
                $incrementalOptions
            );
            $result = $incrementalImporter->importToTable(
                $incrementalLoadStagingTable,
                $destination,
                $incrementalOptions,
                $importState
            );
        } finally {
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $fullLoadStagingTable->getSchemaName(),
                    $fullLoadStagingTable->getTableName()
                )
            );
            $this->connection->executeStatement(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $incrementalLoadStagingTable->getSchemaName(),
                    $incrementalLoadStagingTable->getTableName()
                )
            );
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        if (is_array($expected)) {
            $this->assertSynapseTableEqualsExpected(
                $fullLoadSource,
                $destination,
                $incrementalOptions,
                $expected,
                0
            );
        } else {
            $this->assertSynapseTableExpectedRowCount(
                $destination,
                $incrementalOptions,
                $expected
            );
        }
    }
}
