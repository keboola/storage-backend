<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Exception;
use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class Exporter implements ExporterInterface
{
    public const DEFAULT_ADAPTERS = [
        Storage\ABS\SnowflakeExportAdapter::class,
    ];

    /** @var string[] */
    private $adapters = self::DEFAULT_ADAPTERS;

    /** @var Connection */
    private $connection;

    public function __construct(
        Connection $connection
    ) {
        $this->connection = $connection;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $options
    ): void {
        $adapter = $this->getAdapter($source, $destination);
        $adapter->runCopyCommand($source, $destination, $options);
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): SnowflakeExportAdapterInterface {
        $adapterForUse = null;
        foreach ($this->adapters as $adapter) {
            $ref = new \ReflectionClass($adapter);
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

    public function setAdapters(array $adapters): void
    {
        $this->adapters = $adapters;
    }
}
