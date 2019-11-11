<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ImportState;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;

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
        ImportOptions $importOptions,
        ImportState $importState
    ): int;
}
