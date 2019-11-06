<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter as SnowflakeExporter;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;

class DestinationFile extends BaseFile implements DestinationInterface
{
    public function getBackendExportAdapter(
        ExporterInterface $exporter
    ): BackendExportAdapterInterface {
        switch (true) {
            case $exporter instanceof SnowflakeExporter:
                return new SnowflakeExportAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }
}
