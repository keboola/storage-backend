<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
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
     * @param Storage\Snowflake\Table $source
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $options
    ): void {
        if (!$source instanceof Storage\Snowflake\Table) {
            // php 7.4 to the rescue
            throw new \Exception(sprintf(
                'Source "%s" is invalid only "%s" is supported.',
                get_class($source),
                Storage\Snowflake\Table::class
            ));
        }
        $adapter = $destination->getBackendExportAdapter($this);
        $cmd = $adapter->getCopyCommand($source, $options);
        $this->connection->fetchAll($cmd);
    }
}
