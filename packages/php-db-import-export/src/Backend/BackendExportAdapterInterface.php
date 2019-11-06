<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

interface BackendExportAdapterInterface
{
    public function __construct(Storage\DestinationInterface $destination);

    public function getCopyCommand(
        Storage\SourceInterface $source,
        ExportOptions $exportOptions
    ): string;
}
