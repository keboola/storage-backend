<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse;

use Exception;
use Doctrine\DBAL\Connection;
use Keboola\Db\ImportExport\Backend\ExporterInterface;
use Keboola\Db\ImportExport\ExportOptions;
use Keboola\Db\ImportExport\Storage;

class Exporter implements ExporterInterface
{
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
        if (!$source instanceof Storage\SqlSourceInterface) {
            throw new Exception(sprintf(
                'Source "%s" must implement "%s".',
                get_class($source),
                Storage\SqlSourceInterface::class
            ));
        }
        $adapter = $destination->getBackendExportAdapter($this);
        $adapter->runCopyCommand($source, $options, $this->connection);
    }
}
