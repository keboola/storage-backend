<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Exasol\ExasolImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Exasol\SelectSource;
use Keboola\Db\ImportExport\Storage\Exasol\Table;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableDefinition;
use Keboola\TableBackendUtils\Table\Exasol\ExasolTableReflection;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;

class FromTableInsertIntoAdapter implements CopyAdapterInterface
{
    /** @var Connection */
    private $connection;

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
        assert($destination instanceof ExasolTableDefinition);
        assert($importOptions instanceof ExasolImportOptions);

        $quotedColumns = array_map(static function ($column) {
            return ExasolQuote::quoteSingleIdentifier($column);
        }, $source->getColumnsNames());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            ExasolQuote::quoteSingleIdentifier($destination->getSchemaName()),
            ExasolQuote::quoteSingleIdentifier($destination->getTableName()),
            implode(', ', $quotedColumns),
            $source->getFromStatement()
        );

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->executeStatement($sql);
        }

        $ref = new ExasolTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }
}
