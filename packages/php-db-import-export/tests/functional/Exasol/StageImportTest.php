<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\Storage\Exasol\Table;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
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

    public function testMoveDataFromAToStagingTable(): void
    {
        $this->initTable($this->getSourceSchemaName(), 'sourceTable');
        $this->initTable($this->getDestinationSchemaName(), 'targetTable');

        $importer = new ToStageImporter($this->connection);
        $targetTableRef = new ExasolTableReflection(
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
            $this->getExasolImportOptions()
        );

        $dataSource = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($this->getSourceSchemaName()),
                ExasolQuote::quoteSingleIdentifier('sourceTable')
            )
        );
        $dataDest = $this->connection->fetchAllAssociative(
            sprintf(
                'SELECT * FROM %s.%s',
                ExasolQuote::quoteSingleIdentifier($this->getDestinationSchemaName()),
                ExasolQuote::quoteSingleIdentifier('targetTable')
            )
        );

        self::assertSame($dataSource, $dataDest);
    }

    protected function getExasolImportOptions(): ExasolImportOptions
    {
        return new ExasolImportOptions();
    }
}
