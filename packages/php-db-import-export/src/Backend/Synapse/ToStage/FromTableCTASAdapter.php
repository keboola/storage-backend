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
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
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

        if ($source instanceof Table) {
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

        if ($destination->getTableDistribution()->isHashDistribution()) {
            $quotedColumns = array_map(function ($columnName) {
                return SynapseQuote::quoteSingleIdentifier($columnName);
            }, $destination->getTableDistribution()->getDistributionColumnsNames());
            $distributionSql = sprintf(
                'DISTRIBUTION = %s(%s)',
                $destination->getTableDistribution()->getDistributionName(),
                implode(',', $quotedColumns)
            );
        } else {
            $distributionSql = sprintf(
                'DISTRIBUTION = %s',
                $destination->getTableDistribution()->getDistributionName()
            );
        }

        if ($destination->getTableIndex()->getIndexType() === TableIndexDefinition::TABLE_INDEX_TYPE_CLUSTERED_INDEX) {
            $quotedColumns = array_map(function ($columnName) {
                return SynapseQuote::quoteSingleIdentifier($columnName);
            }, $destination->getTableIndex()->getIndexedColumnsNames());
            $indexSql = sprintf(
                '%s(%s)',
                $destination->getTableIndex()->getIndexType(),
                implode(',', $quotedColumns)
            );
        } else {
            $indexSql = $destination->getTableIndex()->getIndexType();
        }
        $from = $source->getFromStatement();
        if ($source instanceof Table) {
            $from = $source->getFromStatementForStaging(
                $importOptions->getCastValueTypes() === false
            );
        }

        $sql = sprintf(
            'CREATE TABLE %s.%s WITH (%s,%s) AS %s',
            SynapseQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($destination->getTableName()),
            $distributionSql,
            $indexSql,
            $from
        );

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
