<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Exasol;

use Keboola\Db\ImportExport\Backend\Exasol\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Exasol\ToStage\ToStageImporter;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Tests\Keboola\Db\ImportExport\S3SourceTrait;

class FullImportTest extends ExasolBaseTestCase
{
    use S3SourceTrait;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema($this->getDestinationSchemaName());
        $this->cleanSchema($this->getSourceSchemaName());
        $this->createSchema($this->getSourceSchemaName());
        $this->createSchema($this->getDestinationSchemaName());
    }

    public function testLoadToFinalTable(): void
    {
        $this->initTable(self::TABLE_COLUMN_NAME_ROW_NUMBER);

        // skipping header
        $options = $this->getExasolImportOptions(1);
        $source = $this->createS3SourceInstance(
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
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_COLUMN_NAME_ROW_NUMBER
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'row_number',
        ]);
        $qb = new ExasolTableQueryBuilder();
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

        self::assertEquals(2, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithSinglePK(): void
    {
        $this->initTable(self::TABLE_SINGLE_PK);

        // skipping header
        $options = $this->getExasolImportOptions(1);
        $source = $this->createS3SourceInstance(
            'multi-pk.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_SINGLE_PK
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new ExasolTableQueryBuilder();
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

        // TODO @zajca $result ma rowsCount jeste z importu do staging tabulky (6, dva ma duplicitni podle PK).
        // Ale realne se do cilove tabulky importoval jiny pocet radku (4 protoze 2 duplicity). Co se ma vracet?
        self::assertEquals(4, $destinationRef->getRowsCount());
    }

    public function testLoadToTableWithDedupWithMultiPK(): void
    {
        $this->initTable(self::TABLE_MULTI_PK);

        // skipping header
        $options = $this->getExasolImportOptions(1);
        $source = $this->createS3SourceInstance(
            'multi-pk.csv',
            [
                'VisitID',
                'Value',
                'MenuItem',
                'Something',
                'Other',
            ],
            false,
            false,
            ['VisitID', 'Something']
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new ExasolTableReflection(
            $this->connection,
            $this->getDestinationSchemaName(),
            self::TABLE_MULTI_PK
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'VisitID',
            'Value',
            'MenuItem',
            'Something',
            'Other',
        ]);
        $qb = new ExasolTableQueryBuilder();
        $this->connection->executeStatement(
            $qb->getCreateTableCommandFromDefinition($stagingTable)
        );
        $importState = $importer->importToStagingTable(
            $source,
            $stagingTable,
            $options
        );

        // now 6 lines. Add one with same VisitId and Something as an existing line has
        // -> expecting that this line will be skipped when DEDUP
        $this->connection->executeQuery(
            sprintf(
                "INSERT INTO %s.%s VALUES ('134', 'xx', 'yy', 'abc', 'def');",
                ExasolQuote::quoteSingleIdentifier($stagingTable->getSchemaName()),
                ExasolQuote::quoteSingleIdentifier($stagingTable->getTableName())
            )
        );
        $toFinalTableImporter = new FullImporter($this->connection);
        $result = $toFinalTableImporter->importToTable(
            $stagingTable,
            $destination,
            $options,
            $importState
        );

        self::assertEquals(6, $destinationRef->getRowsCount());
    }
}
