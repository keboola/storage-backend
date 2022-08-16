<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Exception;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\BackendExportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;
use ReflectionClass;

class Exporter implements ExporterInterface
{
    public const DEFAULT_ADAPTERS = [
        Storage\S3\SnowflakeExportAdapter::class,
        Storage\ABS\SnowflakeExportAdapter::class,
        Storage\GCS\SnowflakeExportAdapter::class,
    ];

    /** @var string[] */
    private array $adapters = self::DEFAULT_ADAPTERS;

    private Connection $connection;

    public function __construct(
        Connection $connection
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
        ExportOptionsInterface $options
    ): array {
        $adapter = $this->getAdapter($source, $destination);
        return $adapter->runCopyCommand($source, $destination, $options);
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): SnowflakeExportAdapterInterface {
        $adapterForUse = null;
        foreach ($this->adapters as $adapter) {
            $ref = new ReflectionClass($adapter);
            if (!$ref->implementsInterface(SnowflakeExportAdapterInterface::class)) {
                throw new Exception(
                    sprintf(
                        'Each Snowflake export adapter must implement "%s".',
                        SnowflakeExportAdapterInterface::class
                    )
                );
            }
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
                $adapterForUse = new $adapter($this->connection);
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
