<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

interface BackendExportAdapterInterface
{
    public static function isSupported(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): bool;

    public function runCopyCommand(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $exportOptions
    ): void;
}
