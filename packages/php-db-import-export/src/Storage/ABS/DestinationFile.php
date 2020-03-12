<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\Backend\Snowflake\Exporter as SnowflakeExporter;
use Keboola\Db\ImportExport\Backend\Synapse\Exporter as SynapseExporter;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;

class DestinationFile extends BaseFile implements DestinationInterface
{
    /** @var null|string */
    private $blobMasterKey;

    public function __construct(
        string $container,
        string $filePath,
        string $sasToken,
        string $accountName,
        ?string $blobMasterKey = null
    ) {
        parent::__construct($container, $filePath, $sasToken, $accountName);
        $this->blobMasterKey = $blobMasterKey;
    }

    public function getBackendExportAdapter(
        ExporterInterface $exporter
    ): BackendExportAdapterInterface {
        switch (true) {
            case $exporter instanceof SnowflakeExporter:
                return new SnowflakeExportAdapter($this);
            case $exporter instanceof SynapseExporter:
                return new SynapseExportAdapter($this);
            default:
                throw new NoBackendAdapterException();
        }
    }

    public function getBlobMasterKey(): ?string
    {
        return $this->blobMasterKey;
    }
}
