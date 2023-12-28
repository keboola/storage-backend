<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Synapse\SynapseImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Synapse\SelectSource;
use Keboola\Db\ImportExport\Storage\Synapse\Table;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use Keboola\TableBackendUtils\Table\SynapseTableDefinition;
use Keboola\TableBackendUtils\Table\SynapseTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromTableInsertIntoAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions,
    ): int {
        assert($source instanceof SelectSource || $source instanceof Table);
        assert($destination instanceof SynapseTableDefinition);
        assert($importOptions instanceof SynapseImportOptions);

        $quotedColumns = array_map(function ($column) {
            return SynapseQuote::quoteSingleIdentifier($column);
        }, $source->getColumnsNames());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            SynapseQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SynapseQuote::quoteSingleIdentifier($destination->getTableName()),
            implode(', ', $quotedColumns),
            $source->getFromStatement(),
        );

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->executeStatement($sql);
        }

        $ref = new SynapseTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName(),
        );

        return $ref->getRowsCount();
    }
}
