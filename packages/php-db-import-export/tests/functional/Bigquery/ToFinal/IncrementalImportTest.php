<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery\ToFinal;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Bigquery\ToFinalTable\SqlBuilder;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Connection\Bigquery\SessionFactory;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableDefinition;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Tests\Keboola\Db\ImportExportCommon\StorageTrait;
use Tests\Keboola\Db\ImportExportFunctional\Bigquery\BigqueryBaseTestCase;

class IncrementalImportTest extends BigqueryBaseTestCase
{
    protected function getBigqueryIncrementalImportOptions(
        int $skipLines = ImportOptions::SKIP_FIRST_LINE
    ): BigqueryImportOptions {
        return new BigqueryImportOptions(
            [],
            true,
            true,
            $skipLines,
            BigqueryImportOptions::USING_TYPES_STRING
        );
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    /**
     * @return \Generator<string, array<mixed>>
     */
    public function incrementalImportData(): Generator
    {
        // accounts
        $accountsStub = $this->getParseCsvStub('expectation.tw_accounts.increment.csv');
        $accountsNoDedupStub = $this->getParseCsvStub('expectation.tw_accounts.increment.nodedup.csv');
        // multi pk
//        $multiPKStub = $this->getParseCsvStub('expectation.multi-pk_not-null.increment.csv');

        yield 'simple no dedup' => [
            $this->getSourceInstance(
                'tw_accounts.csv',
                $accountsNoDedupStub->getColumns(),
                false,
                false,
                ['id']
            ),
            $this->getSimpleImportOptions(),
            $this->getSourceInstance(
                'tw_accounts.increment.csv',
                $accountsNoDedupStub->getColumns(),
                false,
                false,
                ['id']
            ),
            $this->getBigqueryIncrementalImportOptions(),
            [$this->getDestinationDbName(), 'accounts-3'],
            $accountsNoDedupStub->getRows(),
            4,
            self::TABLE_ACCOUNTS_3,
            [],
        ];
//        yield 'simple' => [
//            $this->getSourceInstance(
//                'tw_accounts.csv',
//                $accountsStub->getColumns(),
//                false,
//                false,
//                ['id']
//            ),
//            $this->getSimpleImportOptions(),
//            $this->getSourceInstance(
//                'tw_accounts.increment.csv',
//                $accountsStub->getColumns(),
//                false,
//                false,
//                ['id']
//            ),
//            $this->getBigqueryIncrementalImportOptions(),
//            [$this->getDestinationDbName(), 'accounts-3'],
//            $accountsStub->getRows(),
//            4,
//            self::TABLE_ACCOUNTS_3,
//        ];
//        yield 'simple no timestamp' => [
//            $this->getSourceInstance(
//                'tw_accounts.csv',
//                $accountsStub->getColumns(),
//                false,
//                false,
//                ['id']
//            ),
//            new BigqueryImportOptions(
//                [],
//                false,
//                false, // disable timestamp
//                ImportOptions::SKIP_FIRST_LINE
//            ),
//            $this->getSourceInstance(
//                'tw_accounts.increment.csv',
//                $accountsStub->getColumns(),
//                false,
//                false,
//                ['id']
//            ),
//            new BigqueryImportOptions(
//                [],
//                true, // incremental
//                false, // disable timestamp
//                ImportOptions::SKIP_FIRST_LINE
//            ),
//            [$this->getDestinationDbName(), 'accounts_without_ts'],
//            $accountsStub->getRows(),
//            4,
//            self::TABLE_ACCOUNTS_WITHOUT_TS,
//        ];
//        yield 'multi pk' => [
//            $this->getSourceInstance(
//                'multi-pk_not-null.csv',
//                $multiPKStub->getColumns(),
//                false,
//                false,
//                ['VisitID', 'Value', 'MenuItem']
//            ),
//            $this->getSimpleImportOptions(),
//            $this->getSourceInstance(
//                'multi-pk_not-null.increment.csv',
//                $multiPKStub->getColumns(),
//                false,
//                false,
//                ['VisitID', 'Value', 'MenuItem']
//            ),
//            $this->getBigqueryIncrementalImportOptions(),
//            [$this->getDestinationDbName(), 'multi_pk_ts'],
//            $multiPKStub->getRows(),
//            3,
//            self::TABLE_MULTI_PK_WITH_TS,
//        ];
    }

    /**
     * @dataProvider  incrementalImportData
     * @param string[] $table
     * @param string[] $dedupCols
     * @param array<mixed> $expected
     */
    public function testIncrementalImport(
        Storage\SourceInterface $fullLoadSource,
        BigqueryImportOptions $fullLoadOptions,
        Storage\SourceInterface $incrementalSource,
        BigqueryImportOptions $incrementalOptions,
        array $table,
        array $expected,
        int $expectedImportedRowCount,
        string $tablesToInit,
        array $dedupCols
    ): void {
        $this->initTable($tablesToInit);

        [$schemaName, $tableName] = $table;
        /** @var BigqueryTableDefinition $destination */
        $destination = (new BigqueryTableReflection(
            $this->bqClient,
            $schemaName,
            $tableName
        ))->getTableDefinition();
        // update PK
        $destination = new BigqueryTableDefinition(
            $schemaName,
            $tableName,
            $destination->isTemporary(),
            $destination->getColumnsDefinitions(),
            $dedupCols
        );

        $toStageImporter = new ToStageImporter($this->bqClient);
        $fullImporter = new FullImporter($this->bqClient);
        $incrementalImporter = new IncrementalImporter($this->bqClient);

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
            $qb = new BigqueryTableQueryBuilder();
            $this->bqClient->runQuery($this->bqClient->query(
                $qb->getCreateTableCommandFromDefinition($fullLoadStagingTable)
            ));

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
            $qb = new BigqueryTableQueryBuilder();
            $this->bqClient->runQuery($this->bqClient->query(
                $qb->getCreateTableCommandFromDefinition($incrementalLoadStagingTable)
            ));
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
            $this->bqClient->runQuery($this->bqClient->query(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $fullLoadStagingTable->getSchemaName(),
                    $fullLoadStagingTable->getTableName()
                )
            ));
            $this->bqClient->runQuery($this->bqClient->query(
                (new SqlBuilder())->getDropTableIfExistsCommand(
                    $incrementalLoadStagingTable->getSchemaName(),
                    $incrementalLoadStagingTable->getTableName()
                )
            ));
        }

        self::assertEquals($expectedImportedRowCount, $result->getImportedRowsCount());

        /** @var BigqueryTableDefinition $destination */
        $this->assertBigqueryTableEqualsExpected(
            $fullLoadSource,
            $destination,
            $incrementalOptions,
            $expected,
            0
        );
    }
}
