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
use Throwable;

class FromTableCTASAdapter implements CopyAdapterInterface
{
    private const TMP_TABLE_SUFFIX = '_tmp_rename';

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

        $tempTableName = $destination->getTableName() . self::TMP_TABLE_SUFFIX;
        try {
            // if temp table exists for some reason from previous run, drop it
            (new SynapseTableReflection(
                $this->connection,
                $destination->getSchemaName(),
                $tempTableName
            ))->getObjectId();
            // rename table to tmp
            $this->connection->executeQuery(
                (new SynapseTableQueryBuilder())->getDropTableCommand(
                    $destination->getSchemaName(),
                    $tempTableName
                )
            );
        } catch (TableNotExistsReflectionException $e) {
            // ignore if table not exists
        }

        $isMainTableTemporary = false;
        try {
            // check if table exists
            $ref = (new SynapseTableReflection(
                $this->connection,
                $destination->getSchemaName(),
                $destination->getTableName()
            ));
            $ref->getObjectId();
            $isMainTableTemporary = $ref->isTemporary();
            if ($isMainTableTemporary) {
                // drop table
                $this->connection->executeQuery(
                    (new SynapseTableQueryBuilder())->getDropTableCommand(
                        $destination->getSchemaName(),
                        $destination->getTableName()
                    )
                );
            } else {
                // rename table to temp
                $this->connection->executeQuery(
                    (new SynapseTableQueryBuilder())->getRenameTableCommand(
                        $destination->getSchemaName(),
                        $destination->getTableName(),
                        $tempTableName
                    )
                );
            }
        } catch (TableNotExistsReflectionException $e) {
            // ignore if table not exists
        }

        $sql = FromTableCTASAdapterSqlBuilder::getCTASCommand($destination, $source, $importOptions);
        $dropTempTable = true;
        try {
            if ($source instanceof SelectSource) {
                $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
            } else {
                $this->connection->executeStatement($sql);
            }
        } catch (Throwable $e) {
            $dropTempTable = false;
            // if ctas fails rename table back
            if ($isMainTableTemporary === false) {
                $this->connection->executeQuery(
                    (new SynapseTableQueryBuilder())->getRenameTableCommand(
                        $destination->getSchemaName(),
                        $tempTableName,
                        $destination->getTableName()
                    )
                );
            }
        }

        if ($dropTempTable === true && $isMainTableTemporary === false) {
            $this->connection->executeQuery(
                (new SynapseTableQueryBuilder())->getDropTableCommand(
                    $destination->getSchemaName(),
                    $tempTableName
                )
            );
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }
}
