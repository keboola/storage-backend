<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional\Teradata;

use Keboola\Db\ImportExport\Backend\Teradata\ToFinalTable\FullImporter;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\StageTableDefinitionFactory;
use Keboola\Db\ImportExport\Backend\Teradata\ToStage\ToStageImporter;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableQueryBuilder;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
use Tests\Keboola\Db\ImportExport\S3SourceTrait;

class FullImportTest extends TeradataBaseTestCase
{
    use S3SourceTrait;

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDestinationDbName());
        $this->createDatabase($this->getDestinationDbName());

        $this->cleanDatabase($this->getSourceDbName());
        $this->createDatabase($this->getSourceDbName());
    }

    public function testLoadToFinalTableWithoutDedup(): void
    {
        // table translations checks numeric and string-ish data
        $this->initTable(self::TABLE_TRANSLATIONS);

        // skipping header
        $options = $this->getImportOptions([], false, false, 1);
        $source = $this->createS3SourceInstance(
            self::TABLE_TRANSLATIONS . '.csv',
            [
                'id',
                'name',
                'price',
                'isDeleted',
            ],
            false,
            false,
            []
        );

        $importer = new ToStageImporter($this->connection);
        $destinationRef = new TeradataTableReflection(
            $this->connection,
            $this->getDestinationDbName(),
            self::TABLE_TRANSLATIONS
        );
        $destination = $destinationRef->getTableDefinition();
        $stagingTable = StageTableDefinitionFactory::createStagingTableDefinition($destination, [
            'id',
            'name',
            'price',
            'isDeleted',
        ]);
        $qb = new TeradataTableQueryBuilder();
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

        self::assertEquals(3, $destinationRef->getRowsCount());
    }

}
