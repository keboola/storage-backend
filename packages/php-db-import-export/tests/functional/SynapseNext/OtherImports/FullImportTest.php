<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\SynapseNext\OtherImports;

use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\SynapseNext\SynapseBaseTestCase;

class FullImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function testImportShouldNotFailOnColumnNameRowNumber(): void
    {
        $this->initTables([self::TABLE_COLUMN_NAME_ROW_NUMBER]);

        $options = $this->getSynapseImportOptions();
        $source = $this->createABSSourceInstance(
            'column-name-row-number.csv',
            [
                'id',
                'row_number',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_COLUMN_NAME_ROW_NUMBER
        );
        $destination = $ref->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'row_number',
        ]);
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(2, $result->getImportedRowsCount());
    }

    public function testNullifyCopy(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', NULL)',
            $this->getSourceSchemaName()
        ));
        $this->connection->query(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', NULL, 500)',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE,
            // @phpstan-ignore-next-line
            getenv('CREDENTIALS_IMPORT_TYPE'),
            // @phpstan-ignore-next-line
            getenv('TEMP_TABLE_TYPE'),
            // @phpstan-ignore-next-line
            getenv('DEDUP_TYPE')
        );
        $source = new Storage\Synapse\Table(
            $this->getSourceSchemaName(),
            'nullify_src',
            ['id', 'name', 'price'],
            []
        );

        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'nullify'
        );
        $destination = $ref->getTableDefinition();
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getSourceSchemaName(),
            'nullify_src'
        );
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $importedData = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            $this->getDestinationSchemaName()
        ));
        self::assertCount(2, $importedData);
        self::assertTrue($importedData[0]['name'] === null);
        self::assertTrue($importedData[0]['price'] === null);
        self::assertTrue($importedData[1]['name'] === null);
    }

    public function testNullifyCsv(): void
    {
        $this->connection->query(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            false,
            false,
            ImportOptions::SKIP_FIRST_LINE,
            // @phpstan-ignore-next-line
            getenv('CREDENTIALS_IMPORT_TYPE'),
            // @phpstan-ignore-next-line
            getenv('TEMP_TABLE_TYPE'),
            // @phpstan-ignore-next-line
            getenv('DEDUP_TYPE')
        );
        $source = $this->createABSSourceInstance(
            'nullify.csv',
            ['id', 'name', 'price'],
            false,
            false,
            []
        );
        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'nullify'
        );
        $destination = $ref->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $destination,
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $importedData = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify] ORDER BY [id] ASC',
            $this->getDestinationSchemaName()
        ));
        self::assertCount(3, $importedData);
        self::assertTrue($importedData[1]['name'] === null);
        self::assertTrue($importedData[2]['price'] === null);
    }
}
