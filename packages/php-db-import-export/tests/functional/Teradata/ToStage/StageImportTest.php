<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata\ToStage;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception\FailedTPTLoadException;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\Exception\NoMoreRoomInTDException;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Schema\Teradata\TeradataSchemaReflection;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Teradata\TeradataBaseTestCase;

class StageImportTest extends TeradataBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase(self::TEST_DATABASE);
        $this->createDatabase(self::TEST_DATABASE);
    }

    public function testSimpleStageImport(): void
    {
        $this->connection->executeQuery(
            sprintf(
                'CREATE MULTISET TABLE %s.%s
     (
      "id" INTEGER NOT NULL,
      "first_name" CHAR(50),
      "last_name" CHAR(50)
     );',
                TeradataQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new TeradataTableReflection(
            $this->connection,
            self::TEST_DATABASE,
            self::TABLE_GENERIC
        );

        $state = $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
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
        $this->connection->executeQuery(
            // table is one column short - import should fail
            sprintf(
                'CREATE MULTISET TABLE %s.%s
     (
      "id" INTEGER NOT NULL,
      "first_name" CHAR(1)
     );',
                TeradataQuote::quoteSingleIdentifier(self::TEST_DATABASE),
                TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
            )
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new TeradataTableReflection(
            $this->connection,
            self::TEST_DATABASE,
            self::TABLE_GENERIC
        );

        try {
            $importer->importToStagingTable(
                $this->createS3SourceInstanceFromCsv('csv/simple/a_b_c-1row.csv', new CsvOptions()),
                $ref->getTableDefinition(),
                $this->getImportOptions(
                    [],
                    false,
                    false,
                    1
                )
            );
            self::fail('should fail');
        } catch (FailedTPTLoadException $e) {
            // nor target table nor LOG/ERR tables should be present
            $scheRef = new TeradataSchemaReflection($this->connection, self::TEST_DATABASE);
            $tables = $scheRef->getTablesNames();
            self::assertCount(0, $tables);
        }
    }

    public function testItWontFitIn(): void
    {
        // trying to immport big table to small DB via TPT -> should fail and throw custom exception
        $dbName = self::TEST_DATABASE . '_small_db';
        $this->cleanDatabase($dbName);
        $this->createDatabase($dbName, '1e5', '1e5');

        $this->initTable(self::BIGGER_TABLE, $dbName);

        $importer = new ToStageImporter($this->connection);
        $ref = new TeradataTableReflection(
            $this->connection,
            $dbName,
            self::BIGGER_TABLE
        );

        $this->expectException(NoMoreRoomInTDException::class);
        $importer->importToStagingTable(
            $this->createS3SourceInstanceFromCsv('big_table.csv', new CsvOptions()),
            $ref->getTableDefinition(),
            $this->getImportOptions()
        );
    }
}
