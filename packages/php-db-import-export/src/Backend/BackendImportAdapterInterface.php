<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

interface BackendImportAdapterInterface
{
    public static function isSupported(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): bool;

    /**
     * @return int number of inserted rows
     */
    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptionsInterface $importOptions,
        string $stagingTableName
    ): int;
}
