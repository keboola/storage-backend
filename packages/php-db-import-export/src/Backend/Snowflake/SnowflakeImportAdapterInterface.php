<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake;

use Keboola\Db\Import\Snowflake\Connection;
use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;

interface SnowflakeImportAdapterInterface extends BackendImportAdapterInterface
{
    public function __construct(Connection $connection);
}
