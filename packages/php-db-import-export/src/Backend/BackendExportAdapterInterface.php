<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Doctrine\DBAL\Connection as DBALConnection;
use Keboola\Db\Import\Snowflake\Connection as SNFLKConnection;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

interface BackendExportAdapterInterface
{
    public function __construct(Storage\DestinationInterface $destination);

    /**
     * @param null|SNFLKConnection|DBALConnection $connection
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        ExportOptions $exportOptions,
        $connection = null
    ): void;
}
