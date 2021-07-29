<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;

class StageImportTest extends ExasolBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->createSchema($this->getDestinationSchemaName());

        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
    }

    public function testLongColumnImport6k(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        if (getenv('TEMP_TABLE_TYPE') === ExasolImportOptions::TEMP_TABLE_COLUMNSTORE
            || getenv('TEMP_TABLE_TYPE') === ExasolImportOptions::TEMP_TABLE_CLUSTERED_INDEX
            || getenv('TEMP_TABLE_TYPE') === ExasolImportOptions::TEMP_TABLE_HEAP_4000
        ) {
            $this->expectException(Exception::class);
            $this->expectExceptionMessage(
                '[SQL Server]Bulk load data conversion error'
            );
        }

        $importer = new ToStageImporter($this->connection);
        $ref = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer->importToStagingTable(
            $this->createABSSourceInstanceFromCsv(
                'long_col_6k.csv',
                new CsvOptions(),
                [
                    'col1',
                    'col2',
                ],
                false,
                false,
                []
            ),
            $stagingTable,
            $this->getExasolImportOptions()
        );

        if (getenv('TEMP_TABLE_TYPE') === ExasolImportOptions::TEMP_TABLE_HEAP) {
            $sql = sprintf(
                'SELECT "col1","col2" FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            );
            $queryResult = array_map(static function ($row) {
                return array_map(static function ($column) {
                    return $column;
                }, array_values($row));
            }, $this->connection->fetchAllAssociative($sql));

            self::assertEquals(4000, strlen($queryResult[0][0]));
        }
    }

}
