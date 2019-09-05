<?php

namespace Keboola\Db\ImportExport\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;

class Snowflake
{
    /** @var Connection */
    private $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function importTableFromFile(
        string $tableName,
        string $fileUrl,
    ): void {

    }
}