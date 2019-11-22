<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;

interface DestinationInterface
{
    public function getBackendExportAdapter(
        ExporterInterface $exporter
    ): BackendExportAdapterInterface;
}
