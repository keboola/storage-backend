<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Bigquery;

use Google\Cloud\Core\Exception\BadRequestException;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Bigquery\ToStage\ToStageImporter;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Schema\Bigquery\BigquerySchemaReflection;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;

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
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC
        );

        $state = $importer->importToStagingTable(
            $this->createGcsSourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
            $ref->getTableDefinition(),
            $this->getImportOptions(
                [],
                false,
                false,
                1
            )
        );

        self::assertEquals(1, $state->getResult()->getImportedRowsCount());
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
                BigqueryQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );
        $this->bqClient->runQuery($query);

        $importer = new ToStageImporter($this->bqClient);
        $ref = new BigqueryTableReflection(
            $this->bqClient,
            self::TEST_DATABASE,
            self::TABLE_GENERIC
        );

        try {
            $importer->importToStagingTable(
                $this->createGCSSourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
                $ref->getTableDefinition(),
                $this->getImportOptions(
                    [],
                    false,
                    false,
                    1
                )
            );
            self::fail('should fail');
        } catch (BadRequestException $e) {
            // nor target table nor LOG/ERR tables should be present
            $scheRef = new BigquerySchemaReflection($this->bqClient, self::TEST_DATABASE);
            $tables = $scheRef->getTablesNames();
            self::assertCount(1, $tables); // table should be present, only import fails
        }
    }
}
