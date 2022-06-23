<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\Snowflake\SelectSource;
use Keboola\Db\ImportExport\Storage\Snowflake\Table;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
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
        ImportOptionsInterface $importOptions
    ): int {
        assert($source instanceof SelectSource || $source instanceof Table);
        assert($destination instanceof SnowflakeTableDefinition);
        assert($importOptions instanceof SnowflakeImportOptions);

        $quotedColumns = array_map(static function ($column) {
            return SnowflakeQuote::quoteSingleIdentifier($column);
        }, $source->getColumnsNames());

        $sql = sprintf(
            'INSERT INTO %s.%s (%s) %s',
            SnowflakeQuote::quoteSingleIdentifier($destination->getSchemaName()),
            SnowflakeQuote::quoteSingleIdentifier($destination->getTableName()),
            implode(', ', $quotedColumns),
            $source->getFromStatement()
        );

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->executeStatement($sql);
        }

        $ref = new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName()
        );

        return $ref->getRowsCount();
    }
}
