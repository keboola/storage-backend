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
     * @param Storage\Snowflake\Table|Storage\Snowflake\SelectSource $source
     */
    public function exportTable(
        Storage\SourceInterface $source,
        Storage\DestinationInterface $destination,
        ExportOptions $options
    ): void {
        if (!$this->isSourceSupported($source)) {
            // php 7.4 to the rescue
            throw new \Exception(sprintf(
                'Source "%s" is invalid only "%s" or "%s" is supported.',
                get_class($source),
                Storage\Snowflake\Table::class,
                Storage\Snowflake\SelectSource::class
            ));
        }
        $adapter = $destination->getBackendExportAdapter($this);
        $cmd = $adapter->getCopyCommand($source, $options);
        if ($source instanceof Storage\Snowflake\SelectSource) {
            $this->connection->fetchAll($cmd, $source->getQueryBindings());
        } else {
            $this->connection->fetchAll($cmd, []);
        }
    }

    private function isSourceSupported(Storage\SourceInterface $source): bool
    {
        return $source instanceof Storage\Snowflake\Table
            || $source instanceof Storage\Snowflake\SelectSource;
    }
}
