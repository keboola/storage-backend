<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage;

interface BackendImportAdapterInterface
{
    public static function isSupported(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): bool;

    /**
     * @return string[]
     */
    public function getCopyCommands(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName
    ): array;
}
