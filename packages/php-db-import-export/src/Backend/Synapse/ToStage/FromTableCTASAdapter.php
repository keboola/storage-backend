<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Synapse\Exception\Assert;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Synapse\SelectSource;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableQueryBuilder;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;

class FromTableCTASAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof SelectSource || $source instanceof Table);
        assert($destination instanceof SynapseTableDefinition);
        assert($importOptions instanceof SynapseImportOptions);

        if ($source instanceof Table && $importOptions->areSameTablesRequired()) {
            // check this only if table is typed so the types are preserved
            Assert::assertSameColumns(
                (new SynapseTableReflection(
                    $this->connection,
                    $source->getSchema(),
                    $source->getTableName()
                ))->getColumnsDefinitions(),
                $destination->getColumnsDefinitions()
            );
        }

        try {
            (new SynapseTableReflection(
                $this->connection,
                $destination->getSchemaName(),
                $destination->getTableName()
            ))->getObjectId();
            $this->connection->executeQuery(
                (new SynapseTableQueryBuilder())->getDropTableCommand(
                    $destination->getSchemaName(),
                    $destination->getTableName()
                )
            );
        } catch (TableNotExistsReflectionException $e) {
            // ignore if table not exists
        }

        $sql = FromTableCTASAdapterSqlBuilder::getCTASCommand($destination, $source, $importOptions);

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->executeStatement($sql);
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }
}
