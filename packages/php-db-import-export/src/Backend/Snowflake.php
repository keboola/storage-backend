<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\CommandBuilder\AbsBuilder;
use Keboola\Db\ImportExport\ImportOptions;
use Keboola\Db\ImportExport\File;

class Snowflake
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function importTableFromFile(
        ImportOptions $importOptions,
        File\Azure $file
    ): void {
        $builder = new AbsBuilder($this->connection);
        $commands = $builder->buildFileCopyCommands($importOptions, $file);
        foreach ($commands as $command) {
            $this->connection->query($command);
        }
    }
}
