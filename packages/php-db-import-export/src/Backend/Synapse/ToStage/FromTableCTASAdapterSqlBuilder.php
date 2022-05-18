<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\Storage\Synapse\SelectSource;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\Db\ImportExport\Storage;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\Synapse\TableIndexDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;

class FromTableCTASAdapterSqlBuilder
{
    /**
     * @param SelectSource|Table $source
     */
    public static function getCTASCommand(
        SynapseTableDefinition $destination,
        Storage\SourceInterface $source,
        SynapseImportOptions $importOptions
    ): string {
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
            // cast values only if table is not typed (tables are not required to be same)
                !$importOptions->areSameTablesRequired()
            );
        }

        return sprintf(
            'CREATE TABLE %s.%s WITH (%s,%s) AS %s',
            SynapseQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($destination->getTableName()),
            $distributionSql,
            $indexSql,
            $from
        );
    }
}
