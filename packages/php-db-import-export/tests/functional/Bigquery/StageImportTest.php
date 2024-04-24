<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryException;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryImportOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\BigqueryInputDataException;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\Bigquery\Table;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Throwable;

class StageImportTest extends BigqueryBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase(self::TEST_DATABASE);
        $this->createDatabase(self::TEST_DATABASE);
    }

    public function testSimpleStageImport(): void
    {
        $query = $this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s (
            `id` INTEGER,
    `first_name` STRING(100),
    `last_name` STRING(100)
);',
                BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
        );

        $state = $importer->importToStagingTable(
            $this->createGcsSourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
            $ref->getTableDefinition(),
            $this->getImportOptions(
                [],
                false,
                false,
                1,
            ),
        );

        self::assertEquals(1, $state->getResult()->getImportedRowsCount());
    }

    public function testStageImportNullBehavior(): void
    {
        $query = $this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s (
            `id` INTEGER,
    `first_name` STRING(100),
    `last_name` STRING(100)
);',
                BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
        );

        $state = $importer->importToStagingTable(
            $this->createGcsSourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
            $ref->getTableDefinition(),
            new BigqueryImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 1,
                importAsNull: ['3', '2'], // two values are passed second is ignored
            ),
        );

        self::assertEquals(1, $state->getResult()->getImportedRowsCount());
        $this->assertSame([
            [
                'id' => 1,
                'first_name' => '2',
                'last_name' => null,
            ],
        ], $this->fetchTable(self::TEST_DATABASE, self::TABLE_GENERIC));
    }

    public function testStageImportNullDefaultNullMarker(): void
    {
        $query = $this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s (
            `id` INTEGER,
    `first_name` STRING(100),
    `last_name` STRING(100)
);',
                BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
        );

        $state = $importer->importToStagingTable(
            $this->createGcsSourceInstanceFromCsv('csv/edge-cases/1row_empty_column.csv', new CsvOptions()),
            $ref->getTableDefinition(),
            new BigqueryImportOptions(
                convertEmptyValuesToNull: [],
                isIncremental: false,
                useTimestamp: false,
                numberOfIgnoredLines: 1,
                importAsNull: [],
            ),
        );

        self::assertEquals(1, $state->getResult()->getImportedRowsCount());
        $this->assertSame([
            [
                'id' => 1,
                'first_name' => '',
                'last_name' => '3',
            ],
        ], $this->fetchTable(self::TEST_DATABASE, self::TABLE_GENERIC));
    }

    public function testAsciiZeroImport(): void
    {
        $query = $this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s (
    `QUERY_ID` STRING(4000),
        `QUERY_TEXT` STRING(4000),
        `DATABASE_NAME` STRING(4000),
        `SCHEMA_NAME` STRING(4000),
        `QUERY_TYPE` STRING(4000),
        `SESSION_ID` STRING(4000),
        `USER_NAME` STRING(4000),
        `ROLE_NAME` STRING(4000),
        `WAREHOUSE_NAME` STRING(4000),
        `WAREHOUSE_SIZE` STRING(4000),
        `WAREHOUSE_TYPE` STRING(4000),
        `QUERY_TAG` STRING(4000),
        `EXECUTION_STATUS` STRING(4000),
        `ERROR_CODE` STRING(4000),
        `ERROR_MESSAGE` STRING(4000),
        `START_TIME` STRING(4000),
        `END_TIME` STRING(4000),
        `TOTAL_ELAPSED_TIME` STRING(4000),
        `COMPILATION_TIME` STRING(4000),
        `EXECUTION_TIME` STRING(4000),
        `QUEUED_PROVISIONING_TIME` STRING(4000),
        `QUEUED_REPAIR_TIME` STRING(4000),
        `QUEUED_OVERLOAD_TIME` STRING(4000),
        `TRANSACTION_BLOCKED_TIME` STRING(4000),
        `OUTBOUND_DATA_TRANSFER_CLOUD` STRING(4000),
        `OUTBOUND_DATA_TRANSFER_REGION` STRING(4000),
        `OUTBOUND_DATA_TRANSFER_BYTES` STRING(4000),
        `CLUSTER_NUMBER` STRING(4000),
        `BYTES_SCANNED` STRING(4000),
        `ROWS_PRODUCED` STRING(4000),
        `INBOUND_DATA_TRANSFER_CLOUD` STRING(4000),
        `INBOUND_DATA_TRANSFER_REGION` STRING(4000),
        `INBOUND_DATA_TRANSFER_BYTES` STRING(4000),
        `CREDITS_USED_CLOUD_SERVICES` STRING(4000),
);',
                BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
        );

        $state = $importer->importToStagingTable(
            $this->createGcsSourceInstanceFromCsv('csv/edge-cases/ascii_zero.csv.gz', new CsvOptions()),
            $ref->getTableDefinition(),
            $this->getImportOptions(
                [],
                false,
                false,
                1,
            ),
        );

        self::assertEquals(3, $state->getResult()->getImportedRowsCount());
    }

    public function testFailingImport(): void
    {
        $query = $this->bqClient->query(
        // table is one column short - import should fail
            sprintf(
                'CREATE TABLE %s.%s
     (
      `id` INT64 NOT NULL,
      `first_name` STRING(1)
     );',
                BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
        );

        try {
            $importer->importToStagingTable(
                $this->createGCSSourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
                $ref->getTableDefinition(),
                $this->getImportOptions(
                    [],
                    false,
                    false,
                    1,
                ),
            );
            self::fail('should fail');
        } catch (BigqueryException $e) {
            // nor target table nor LOG/ERR tables should be present
            $scheRef = new BigquerySchemaReflection($this->bqClient, self::TEST_DATABASE);
            $tables = $scheRef->getTablesNames();
            self::assertCount(1, $tables); // table should be present, only import fails
        }
    }

    public function testLoadFromFileWithNullValueToRequiredColumn(): void
    {
        $query = $this->bqClient->query(
        // name is NOT NULL, but nullify file contains null value for this column
            sprintf(
                'CREATE TABLE %s.%s
     (
      `id` INT64 NOT NULL,
      `name` STRING(100) NOT NULL,
      `price` STRING(100)
     );',
                BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC,
        );

        try {
            $importer->importToStagingTable(
                $this->createGCSSourceInstanceFromCsv('nullify.csv', new CsvOptions()),
                $ref->getTableDefinition(),
                $this->getImportOptions(
                    [],
                    false,
                    false,
                    1,
                ),
            );
            self::fail('should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(BigqueryInputDataException::class, $e);
        }
    }

    public function testLoadFromTableWithNullValueToRequiredColumn(): void
    {
        $sourceTable = self::TABLE_GENERIC . '_source';
        $destinationTable = self::TABLE_GENERIC . '_dest';

        $destinationPath = [
            BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
            BigqueryQuote::quoteSingleIdentifier($destinationTable),
        ];
        $sourcePath = [
            BigqueryQuote::quoteSingleIdentifier(self::TEST_DATABASE),
            BigqueryQuote::quoteSingleIdentifier($sourceTable),
        ];

        // create destination with NOT NULL
        $query = $this->bqClient->query(
            sprintf(
                'CREATE TABLE %s.%s
     (
      `id` INT64 NOT NULL,
      `name` STRING(100) NOT NULL,
      `price` STRING(100)
     );',
                ...$destinationPath,
            ),
        );
        $this->bqClient->runQuery($query);

        // create source table
        $query = $this->bqClient->query(
        // table is one column short - import should fail
            sprintf(
                'CREATE TABLE %s.%s
     (
      `id` INT64 NOT NULL,
      `name` STRING(100),
      `price` STRING(100)
     );',
                ...$sourcePath,
            ),
        );
        $this->bqClient->runQuery($query);

        // load source table with some NULLs
        $query = $this->bqClient->query(
            sprintf(
                'INSERT INTO %s.%s VALUES (1, NULL, \'1000\');',
                ...$sourcePath,
            ),
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            $destinationTable,
        );

        try {
            // load SOURCE which contains NULL to DESTINATION which has NOT NULL column
            $importer->importToStagingTable(
                new Table(
                    self::TEST_DATABASE,
                    $sourceTable,
                    [],
                    [],
                ),
                $ref->getTableDefinition(),
                $this->getImportOptions(
                    [],
                    false,
                    false,
                    1,
                ),
            );
            self::fail('should fail');
        } catch (Throwable $e) {
            $this->assertInstanceOf(BigqueryInputDataException::class, $e);
        }
    }
}
