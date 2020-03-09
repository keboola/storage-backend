<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\Helper;

final class BackendHelper
{
    public static function generateTempTableName(): string
    {
        return '#__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }
}
