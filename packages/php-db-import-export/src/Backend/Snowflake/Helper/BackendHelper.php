<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

final class BackendHelper
{
    public static function generateStagingTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }
}
