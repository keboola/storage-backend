<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Synapse\Helper;

final class BackendHelper
{
    /**
     * Generates whole temp table name including # prefix
     */
    public static function generateTempTableName(): string
    {
        return '#__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    /**
     * Generates random temp table prefix without # for normal tables
     */
    public static function generateRandomTablePrefix(): string
    {
        return str_replace('.', '_', uniqid('tmp', true));
    }
}
