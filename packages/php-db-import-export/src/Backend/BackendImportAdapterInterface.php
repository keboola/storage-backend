<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\Storage\DestinationInterface;

interface BackendImportAdapterInterface
{
    /**
     * @param null|Connection|\Doctrine\DBAL\Connection $connection
     * @return string[]
     */
    public function getCopyCommands(
        DestinationInterface $destination,
        ImportOptions $importOptions,
        string $stagingTableName,
        $connection = null
    ): array;
}
