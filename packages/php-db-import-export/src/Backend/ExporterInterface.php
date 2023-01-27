<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

interface ExporterInterface
{
    /**
     * @return array<mixed>
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $options
    ): array;

    /**
     * @param class-string<BackendExportAdapterInterface>[] $adapters
     */
    public function setAdapters(array $adapters): void;
}
