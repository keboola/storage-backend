<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Synapse\OtherImports;

use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToFinalTable\IncrementalImporter;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Synapse\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Synapse\SynapseBaseTestCase;

class IncrementalImportTest extends SynapseBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema($this->getDestinationSchemaName());
        $this->dropAllWithinSchema($this->getSourceSchemaName());
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getDestinationSchemaName()));
        $this->connection->exec(sprintf('CREATE SCHEMA [%s]', $this->getSourceSchemaName()));
    }

    public function testIncrementalImportFromTable(): void
    {
        $this->initTables([self::TABLE_OUT_CSV_2COLS]);
        $fetchSQL = sprintf(
            'SELECT [col1], [col2] FROM [%s].[%s]',
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );

        $source = new Storage\Synapse\SelectSource(
            sprintf('SELECT * FROM [%s].[%s]', $this->getSourceSchemaName(), self::TABLE_OUT_CSV_2COLS),
            [],
            [],
            [
                'col1',
                'col2',
            ],
            []
        );
        $options = $this->getSynapseImportOptions();
        $importer = new ToStageImporter($this->connection);
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_OUT_CSV_2COLS
        );
        $destination = $ref->getTableDefinition();
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
        $importedData = $this->connection->fetchAllAssociative($fetchSQL);

        self::assertCount(2, $importedData);

        $this->connection->exec(sprintf('INSERT INTO [%s].[out.csv_2Cols] VALUES
                (\'e\', \'f\');
        ', $this->getSourceSchemaName()));
        $this->connection->executeStatement(
            $qb->getDropTableCommand($stagingTable->getSchemaName(), $stagingTable->getTableName())
        );
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $importedData = $this->connection->fetchAllAssociative($fetchSQL);

        self::assertCount(3, $importedData);
    }

    public function testNullifyCopyIncremental(): void
    {
        $this->connection->executeQuery(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, 50)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', NULL)',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', NULL, 500)',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
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
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'nullify'
        );
        $destination = $ref->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer = new ToStageImporter($this->connection);
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new IncrementalImporter($this->connection);
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
        $this->assertCount(3, $importedData);
        $this->assertTrue($importedData[0]['name'] === null);
        $this->assertTrue($importedData[0]['price'] === null);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['name'] === null);
    }

    public function testNullifyCopyIncrementalWithPk(): void
    {
        $this->connection->executeQuery(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', \'3\', 2)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000) NOT NULL, [name] nvarchar(4000) NOT NULL, [price] nvarchar(4000) NOT NULL, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'4\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
            false,
            SynapseImportOptions::SKIP_FIRST_LINE,
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
            ['id']
        );
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'nullify'
        );
        $destination = $ref->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithText(
            $ref->getTableDefinition(),
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer = new ToStageImporter($this->connection);
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new IncrementalImporter($this->connection);
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $importedData = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify]',
            $this->getDestinationSchemaName()
        ));
        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => null,
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCopyIncrementalWithPkDestinationWithNull(): void
    {
        $this->connection->executeQuery(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, NULL)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
        // phpcs:ignore
            'CREATE TABLE [%s].[nullify_src] ([id] nvarchar(4000) NOT NULL, [name] nvarchar(4000) NOT NULL, [price] nvarchar(4000) NOT NULL, PRIMARY KEY NONCLUSTERED([id]) NOT ENFORCED)',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'1\', \'\', \'\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'2\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify_src] VALUES(\'4\', \'\', \'500\')',
            $this->getSourceSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
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
            ['id']
        );
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'nullify'
        );
        $destination = $ref->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinitionWithText(
            $ref->getTableDefinition(),
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer = new ToStageImporter($this->connection);
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new IncrementalImporter($this->connection);
        $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        $importedData = $this->connection->fetchAllAssociative(sprintf(
            'SELECT [id], [name], [price] FROM [%s].[nullify]',
            $this->getDestinationSchemaName()
        ));
        $expectedData = [
            [
                'id' => '1',
                'name' => null,
                'price' => null,
            ],
            [
                'id' => '2',
                'name' => null,
                'price' => '500',
            ],
            [
                'id' => '4',
                'name' => null,
                'price' => '500',
            ],

        ];

        $this->assertArrayEqualsSorted($expectedData, $importedData, 'id');
    }

    public function testNullifyCsvIncremental(): void
    {
        $this->connection->executeQuery(sprintf(
            'CREATE TABLE [%s].[nullify] ([id] nvarchar(4000), [name] nvarchar(4000), [price] NUMERIC)',
            $this->getDestinationSchemaName()
        ));
        $this->connection->executeQuery(sprintf(
            'INSERT INTO [%s].[nullify] VALUES(\'4\', NULL, 50)',
            $this->getDestinationSchemaName()
        ));

        $options = new SynapseImportOptions(
            ['name', 'price'], //convert empty values
            true, // incremetal
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
        $ref = new SynapseTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'nullify'
        );
        $destination = $ref->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition(
            $ref->getTableDefinition(),
            $source->getColumnsNames()
        );
        $qb = new SynapseTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importer = new ToStageImporter($this->connection);
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );
        $toFinalTableImporter = new IncrementalImporter($this->connection);
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
        $this->assertCount(4, $importedData);
        $this->assertTrue($importedData[1]['name'] === null);
        $this->assertTrue($importedData[2]['price'] === null);
        $this->assertTrue($importedData[3]['name'] === null);
    }
}
