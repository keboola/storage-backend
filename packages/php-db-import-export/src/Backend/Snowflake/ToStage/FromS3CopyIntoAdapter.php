<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\ToStage;

use Doctrine\DBAL\Connection;
use Generator;
use Keboola\Db\ImportExport\Backend\CopyAdapterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\SnowflakeImportOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableDefinition;
use Keboola\TableBackendUtils\Table\TableDefinitionInterface;
use Throwable;

class FromS3CopyIntoAdapter implements CopyAdapterInterface
{
    private Connection $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    /**
     * @param SourceFile $source
     * @param SnowflakeTableDefinition $destination
     * @param SnowflakeImportOptions $importOptions
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        TableDefinitionInterface $destination,
        ImportOptionsInterface $importOptions
    ): int {
// todo
    }

    /**
     * @return \Generator<string|null>
     */
    private function getCopyCommand(
        Storage\S3\SourceFile $source,
        SnowflakeTableDefinition $destination,
        SnowflakeImportOptions $importOptions
    ): Generator {
        // TODO
    }
}
