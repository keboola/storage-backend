<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Doctrine\DBAL\Connection;
use Exception;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\ExportOptionsInterface;
use Keboola\Db\ImportExport\Storage;

class Exporter implements ExporterInterface
{
    public const DEFAULT_ADAPTERS = [
        Storage\ABS\SynapseExportAdapter::class,
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

    public function setAdapters(array $adapters): void
    {
        $this->adapters = $adapters;
    }

    /**
     * @param Storage\SqlSourceInterface $source
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptionsInterface $options
    ): void {
        $adapter = $this->getAdapter($source, $destination);
        $adapter->runCopyCommand($source, $destination, $options);
    }

    private function getAdapter(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination
    ): SynapseExportAdapterInterface {
        $adapterForUse = null;
        foreach ($this->adapters as $adapter) {
            $ref = new \ReflectionClass($adapter);
            if (!$ref->implementsInterface(SynapseExportAdapterInterface::class)) {
                throw new Exception(
                    sprintf(
                        'Each Synapse export adapter must implement "%s".',
                        SynapseExportAdapterInterface::class
                    )
                );
            }
            if ($adapter::isSupported($source, $destination)) {
                if ($adapterForUse !== null) {
                    throw new Exception(
                        sprintf(
                            'More than one suitable adapter found for Synapse exporter with source: '
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
                    'No suitable adapter found for Synapse exporter with source: "%s", destination "%s".',
                    get_class($source),
                    get_class($destination)
                )
            );
        }

        return $adapterForUse;
    }
}
