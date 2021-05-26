<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\SynapseNext\OtherImports;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseException;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\SynapseNext\SynapseBaseTestCase;

class StageImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function testInvalidFieldQuote(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $file = new CsvFile(self::DATA_DIR . 'escaping/standard-with-enclosures.csv');
        $expectedEscaping = [];
        foreach ($file as $row) {
            $expectedEscaping[] = $row;
        }
        $escapingHeader = array_shift($expectedEscaping); // remove header

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );

        $this->expectException(\Throwable::class);
        $this->expectExceptionMessage('CSV property FIELDQUOTE|ECLOSURE must be set when using Synapse analytics.');
        $importer->importToStagingTable(
            $this->createABSSourceInstanceFromCsv('raw.rs.csv', new CsvOptions("\t", '', '\\')),
            StageTableDefinitionFactory::createStagingTableDefinition($ref->getTableDefinition(), $escapingHeader),
            $this->getSynapseImportOptions()
        );
    }

    public function testCopyInvalidSourceDataShouldThrowException(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $source = new Storage\Synapse\Table(
            $this->getSourceSchemaName(),
            'names',
            ['c1', 'c2'],
            []
        );
        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::COLUMNS_COUNT_NOT_MATCH);
        $importer->importToStagingTable(
            $source,
            StageTableDefinitionFactory::createStagingTableDefinition(
                $ref->getTableDefinition(),
                $ref->getColumnsNames()
            ),
            $this->getSynapseImportOptions()
        );
    }

    public function testInvalidManifestImport(): void
    {
        $this->initTables([self::TABLE_ACCOUNTS_3]);

        $initialFile = new CsvFile(self::DATA_DIR . 'tw_accounts.csv');
        $source = $this->createABSSourceInstance(
            '02_tw_accounts.csv.invalid.manifest',
            $initialFile->getHeader(),
            true,
            false,
            ['id']
        );
        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_ACCOUNTS_3
        );

        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::MANDATORY_FILE_NOT_FOUND);
        $importer->importToStagingTable(
            $source,
            StageTableDefinitionFactory::createStagingTableDefinition(
                $ref->getTableDefinition(),
                $ref->getColumnsNames()
            ),
            $this->getSynapseImportOptions()
        );
    }

    public function testMoreColumnsShouldThrowException(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $source = $this->createABSSourceInstance(
            'tw_accounts.csv',
            [
                'first',
                'second',
            ],
            false,
            false,
            ['id']
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $this->expectException(Exception::class);
        $this->expectExceptionCode(Exception::COLUMNS_COUNT_NOT_MATCH);
        $this->expectExceptionMessage('first');
        $this->expectExceptionMessage('second');
        $importer->importToStagingTable(
            $source,
            StageTableDefinitionFactory::createStagingTableDefinition(
                $ref->getTableDefinition(),
                $ref->getColumnsNames()
            ),
            $this->getSynapseImportOptions()
        );
    }

    public function testLongColumnImport6k(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        if (getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_COLUMNSTORE
            || getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_CLUSTERED_INDEX
            || getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_HEAP_4000
        ) {
            $this->expectException(Exception::class);
            $this->expectExceptionMessage(
                '[SQL Server]Bulk load data conversion error'
            );
        }

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
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
            $this->getSynapseImportOptions()
        );

        if (getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_HEAP) {
            $sql = sprintf(
                'SELECT [col1],[col2] FROM [%s].[%s]',
                $stagingTable->getSchemaName(),
                $stagingTable->getTableName()
            );
            $queryResult = array_map(function ($row) {
                return array_map(function ($column) {
                    return $column;
                }, array_values($row));
            }, $this->connection->fetchAll($sql));

            $this->assertEquals(4000, strlen($queryResult[0][0]));
        }
    }

    public function testLongColumnImport10k(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        if (getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_COLUMNSTORE
            || getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_CLUSTERED_INDEX
            || getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_HEAP_4000
        ) {
            $this->expectException(Exception::class);
            $this->expectExceptionMessage(
                '[SQL Server]Bulk load data conversion error'
            );
        }
        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer->importToStagingTable(
            $this->createABSSourceInstanceFromCsv(
                'long_col_10k.csv',
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
            $this->getSynapseImportOptions()
        );

        if (getenv('TEMP_TABLE_TYPE') === SynapseImportOptions::TEMP_TABLE_HEAP) {
            $sql = sprintf(
                'SELECT [col1],[col2] FROM [%s].[%s]',
                $stagingTable->getSchemaName(),
                $stagingTable->getTableName()
            );
            $queryResult = array_map(function ($row) {
                return array_map(function ($column) {
                    return $column;
                }, array_values($row));
            }, $this->connection->fetchAll($sql));

            $this->assertEquals(4000, strlen($queryResult[0][0]));
        }
    }

    public function testCopyIntoInvalidTypes(): void
    {
        $this->initTables([self::TABLE_TYPES]);

        $source = $this->createABSSourceInstance(
            'typed_table.invalid-types.csv',
            [
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TYPES
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            '[SQL Server]Bulk load data conversion error'
        );
        $importer->importToStagingTable(
            $source,
            $stagingTable,
            $this->getSynapseImportOptions()
        );
    }

    public function testInsertIntoInvalidTypes(): void
    {
        $this->initTables([self::TABLE_TYPES]);

        $source = new Storage\Synapse\Table(
            $this->getDestinationSchemaName(),
            self::TABLE_TYPES,
            [
                'charCol',
                'numCol',
                'floatCol',
                'boolCol',
            ]
        );
        $this->connection->exec(sprintf(
            'INSERT INTO [%s].[types] VALUES
              (\'a\', \'test\', \'test\', 1, \'\')
           ;',
            $this->getDestinationSchemaName()
        ));

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_TYPES
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $ref->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            '[SQL Server]Error converting data type'
        );
        $importer->importToStagingTable(
            $source,
            $stagingTable,
            $this->getSynapseImportOptions()
        );
    }
}
