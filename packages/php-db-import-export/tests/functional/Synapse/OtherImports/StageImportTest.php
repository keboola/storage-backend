<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse\OtherImports;

use Keboola\Csv\CsvFile;
use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Synapse\SynapseBaseTestCase;
use Throwable;

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

        $this->expectException(Throwable::class);
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

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            '[SQL Server]Bulk load data conversion error'
        );

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
    }

    public function testLongColumnImport10k(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            '[SQL Server]Bulk load data conversion error'
        );
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


    public function testInsertIntoColumnsCountMismatch(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);

        $source = new Storage\Synapse\Table(
            $this->getSourceSchemaName(),
            self::TABLE_OUT_CSV_2COLS,
            [
                'col1',
                'col2',
                'invalidCol',
            ]
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            [...$ref->getColumnsNames(), 'invalidCol']
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
        // phpcs:ignore
            'Tables don\'t have same number of columns. Source columns: "col1,col2", Destination columns: "col1,col2,invalidCol"'
        );
        $importer->importToStagingTable(
            $source,
            $stagingTable,
            $this->getSynapseImportOptions(
                ImportOptions::SKIP_FIRST_LINE,
                null,
                SynapseImportOptions::SAME_TABLES_REQUIRED,
                SynapseImportOptions::TABLE_TO_TABLE_ADAPTER_CTAS
            )
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
            'Source destination columns mismatch. "numCol NVARCHAR(4000) NOT NULL"->"numCol DECIMAL(10,1)"'
        );
        $importer->importToStagingTable(
            $source,
            $stagingTable,
            $this->getSynapseImportOptions(
                ImportOptions::SKIP_FIRST_LINE,
                null,
                SynapseImportOptions::SAME_TABLES_REQUIRED,
                SynapseImportOptions::TABLE_TO_TABLE_ADAPTER_CTAS
            )
        );
    }
}
