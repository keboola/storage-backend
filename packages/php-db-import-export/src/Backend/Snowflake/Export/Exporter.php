<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Doctrine\DBAL\Connection;
use Exception;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class Exporter implements ExporterInterface
{
    public const DEFAULT_ADAPTERS = [
        S3ExportAdapter::class,
        AbsExportAdapter::class,
        GcsExportAdapter::class,
    ];

    /** @var array<class-string<BackendExportAdapterInterface>> */
    private array $adapters = self::DEFAULT_ADAPTERS;

    private Connection $connection;

    public function __construct(
        Connection $connection,
    ) {
        $this->connection = $connection;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     * @return array<mixed>
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $options,
    ): array {
        return $this->getAdapter($source, $destination)
            ->runCopyCommand($source, $destination, $options);
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
    ): BackendExportAdapterInterface {
        $adapterForUse = null;
        foreach ($this->adapters as $adapter) {
            if ($adapter::isSupported($source, $destination)) {
                if ($adapterForUse !== null) {
                    throw new Exception(
                        sprintf(
                            'More than one suitable adapter found for Snowflake exporter with source: '
                            . '"%s", destination "%s".',
                            get_class($source),
                            get_class($destination),
                        ),
                    );
                }
                $adapterForUse = new $adapter($this->connection);
            }
        }
        if ($adapterForUse === null) {
            throw new Exception(
                sprintf(
                    'No suitable adapter found for Snowflake exporter with source: "%s", destination "%s".',
                    get_class($source),
                    get_class($destination),
                ),
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
