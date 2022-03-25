<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Teradata\TeradataImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Teradata\SelectSource;
use Keboola\Db\ImportExport\Storage\Teradata\Table;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableDefinition;
use Keboola\TableBackendUtils\Table\Teradata\TeradataTableReflection;
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
        assert($destination instanceof TeradataTableDefinition);
        assert($importOptions instanceof TeradataImportOptions);

        $quotedColumns = array_map(static function ($column) {
            return TeradataQuote::quoteSingleIdentifier($column);
        }, $destination->getColumnsNames());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            TeradataQuote::quoteSingleIdentifier($destination->getSchemaName()),
            TeradataQuote::quoteSingleIdentifier($destination->getTableName()),
            implode(', ', $quotedColumns),
            $source->getFromStatement()
        );

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->executeStatement($sql);
        }

        $ref = new TeradataTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }
}
