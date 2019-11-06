<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Storage\DestinationInterface;

interface SnowflakeImportAdapterInterface extends BackendImportAdapterInterface
{
    /**
     * Snowflake import is handled differently for copy table2table and file2table
     *
     * @param string[] $commands - sql commands array
     * @return int - number of imported rows
     */
    public function executeCopyCommands(
        array $commands,
        Connection $connection,
        DestinationInterface $destination,
        ImportOptions $importOptions,
        ImportState $importState
    ): int;
}
