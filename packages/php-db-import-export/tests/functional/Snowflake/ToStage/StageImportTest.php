<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Snowflake\ToStage;

use Keboola\Db\Import\Exception;
use Keboola\Db\ImportExport\Backend\Snowflake\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Tests\Keboola\Db\ImportExportFunctional\Snowflake\SnowflakeBaseTestCase;

class StageImportTest extends SnowflakeBaseTestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->createSchema($this->getDestinationSchemaName());

        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
    }

    public function testMoveDataFromAToB(): void
    {
        $this->initSingleTable($this->getSourceSchemaName(), 'sourceTable');
        $this->initSingleTable($this->getDestinationSchemaName(), 'targetTable');

        $importer = new ToStageImporter($this->connection);
        $targetTableRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'targetTable'
        );

        $source = new Table(
            $this->getSourceSchemaName(),
            'sourceTable',
            ['id', 'first_name', 'last_name'],
            []
        );

        $this->insertRowToTable($this->getSourceSchemaName(), 'sourceTable', 1, 'a', 'b');
        $this->insertRowToTable($this->getSourceSchemaName(), 'sourceTable', 2, 'c', 'd');

        $importer->importToStagingTable(
            $source,
            $targetTableRef->getTableDefinition(),
            $this->getSnowflakeImportOptions()
        );

        $dataSource = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier('sourceTable')
            )
        );
        $dataDest = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier('targetTable')
            )
        );

        self::assertSame($dataSource, $dataDest);
    }

    public function testMoveDataFromAToTableWithWrongSourceStructure(): void
    {
        $this->initSingleTable($this->getDestinationSchemaName(), 'targetTable');

        $importer = new ToStageImporter($this->connection);
        $targetTableRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'targetTable'
        );

        $source = new Table(
            $this->getSourceSchemaName(),
            'sourceTable',
            ['id', 'XXXX', 'last_name'],
            []
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Columns doest not match. Non existing columns: XXX');

        $importer->importToStagingTable(
            $source,
            $targetTableRef->getTableDefinition(),
            $this->getSnowflakeImportOptions()
        );
    }

    public function testMoveDataFromBetweenDifferentTables(): void
    {
        $this->initSingleTable($this->getSourceSchemaName(), 'sourceTable');
        $this->initSingleTable($this->getDestinationSchemaName(), 'targetTable');

        $this->connection->executeQuery(
            sprintf(
                'ALTER TABLE %s.%s DROP COLUMN %s',
                SnowflakeQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                SnowflakeQuote::quoteSingleIdentifier('targetTable'),
                SnowflakeQuote::quoteSingleIdentifier('first_name')
            )
        );

        $this->insertRowToTable($this->getSourceSchemaName(), 'sourceTable', 1, 'a', 'b');

        $importer = new ToStageImporter($this->connection);
        $targetTableRef = new SnowflakeTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            'targetTable'
        );

        $source = new Table(
            $this->getSourceSchemaName(),
            'sourceTable',
            ['id', 'first_name', 'last_name'],
            []
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage('Columns doest not match. Non existing columns: first_name');

        $importer->importToStagingTable(
            $source,
            $targetTableRef->getTableDefinition(),
            $this->getSnowflakeImportOptions()
        );
    }
}
