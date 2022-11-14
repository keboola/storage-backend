<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\Export;

use Exception;
use Google\Cloud\BigQuery\BigQueryClient;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class Exporter implements ExporterInterface
{
    /** @var array<class-string<BackendExportAdapterInterface>> */
    public const DEFAULT_ADAPTERS = [
        GcsExportAdapter::class,
    ];

    /** @var array<class-string<BackendExportAdapterInterface>> */
    private array $adapters = self::DEFAULT_ADAPTERS;

    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @return array<mixed>
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $options
    ): array {
        return $this->getAdapter($source, $destination)
            ->runCopyCommand($source, $destination, $options);
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): BackendExportAdapterInterface {
        $adapterForUse = null;
        /** @var class-string<BackendExportAdapterInterface> $adapter */
        foreach ($this->adapters as $adapter) {
            if ($adapter::isSupported($source, $destination)) {
                if ($adapterForUse !== null) {
                    throw new Exception(
                        sprintf(
                            'More than one suitable adapter found for Snowflake exporter with source: '
                            . '"%s", destination "%s".',
                            get_class($source),
                            get_class($destination)
                        )
                    );
                }
                $adapterForUse = new $adapter($this->bqClient);
            }
        }
        if ($adapterForUse === null) {
            throw new Exception(
                sprintf(
                    'No suitable adapter found for Snowflake exporter with source: "%s", destination "%s".',
                    get_class($source),
                    get_class($destination)
                )
            );
        }

        return $adapterForUse;
    }

    /**
     * @param class-string<BackendExportAdapterInterface>[] $adapters
     */
    public function setAdapters(array $adapters): void
    {
        $this->adapters = $adapters;
    }
}
