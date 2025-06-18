<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\Db\ImportExport\Backend\Assert;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\Exception\ColumnsMismatchException;
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

    /**
     * @throws ColumnsMismatchException
     * @throws Exception
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions,
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
            $source->getFromStatement(),
        );

        if ($source instanceof Table && $importOptions->isRequireSameTables()) {
            Assert::assertSameColumns(
                source: (new SnowflakeTableReflection(
                    $this->connection,
                    $source->getSchema(),
                    $source->getTableName(),
                ))->getColumnsDefinitions(),
                destination: $destination->getColumnsDefinitions(),
                ignoreSourceColumns: $importOptions->ignoreColumns(),
                complexLengthTypes: [
                    Snowflake::TYPE_NUMBER,
                    Snowflake::TYPE_DECIMAL,
                    Snowflake::TYPE_NUMERIC,
                ],
                simpleLengthTypes: [
                    Snowflake::TYPE_VARCHAR,
                    Snowflake::TYPE_CHAR,
                    Snowflake::TYPE_CHARACTER,
                    Snowflake::TYPE_STRING,
                    Snowflake::TYPE_TEXT,

                    Snowflake::TYPE_TIME,
                    Snowflake::TYPE_DATETIME,
                    Snowflake::TYPE_TIMESTAMP,
                    Snowflake::TYPE_TIMESTAMP_NTZ,
                    Snowflake::TYPE_TIMESTAMP_LTZ,
                    Snowflake::TYPE_TIMESTAMP_TZ,

                    Snowflake::TYPE_FLOAT,
                    Snowflake::TYPE_FLOAT4,
                    Snowflake::TYPE_FLOAT8,
                    Snowflake::TYPE_REAL,

                    Snowflake::TYPE_BINARY,
                    Snowflake::TYPE_VARBINARY,

                ],
            );
        }

        if ($source instanceof SelectSource) {
            $this->connection->executeQuery($sql, $source->getQueryBindings(), $source->getDataTypes());
        } else {
            $this->connection->executeStatement($sql);
        }

        $ref = new SnowflakeTableReflection(
            $this->connection,
            $destination->getSchemaName(),
            $destination->getTableName(),
        );

        return $ref->getRowsCount();
    }
}
