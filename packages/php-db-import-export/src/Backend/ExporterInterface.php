<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

interface ExporterInterface
{
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $options
    ): void;

    public function setAdapters(array $adapters): void;
}
