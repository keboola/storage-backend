<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse;

use Generator;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;

class IncrementalImportNoTypesTest extends SynapseBaseTestCase
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
    public function incrementalImportData(): Generator
    {
        [
            $accountColumns,
            $expectedAccountsRows,
        ] = $this->getExpectationFileData(
            'expectation.tw_accounts.increment.csv',
            self::EXPECTATION_FILE_DATA_KEEP_AS_IS
        );
        [
            $multiPkColumns,
        ] = $this->getExpectationFileData(
            'expectation.multi-pk.increment.csv',
            self::EXPECTATION_FILE_DATA_KEEP_AS_IS
        );

        $tests = [];
        yield 'simple' => [
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
        yield 'simple no timestamp' => [
            $this->createABSSourceInstance(
                'tw_accounts.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            new SynapseImportOptions(
                [],
                false,
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
                // @phpstan-ignore-next-line
                getenv('CREDENTIALS_IMPORT_TYPE')
            ),
            $this->createABSSourceInstance(
                'tw_accounts.increment.csv',
                $accountColumns,
                false,
                false,
                ['id']
            ),
            new SynapseImportOptions(
                [],
                true, // incremental
                false, // disable timestamp
                ImportOptions::SKIP_FIRST_LINE,
                // @phpstan-ignore-next-line
                getenv('CREDENTIALS_IMPORT_TYPE')
            ),
            [$this->getDestinationSchemaName(), 'accounts-bez-ts'],
            $expectedAccountsRows,
            4,
            [self::TABLE_ACCOUNTS_WITHOUT_TS],
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
