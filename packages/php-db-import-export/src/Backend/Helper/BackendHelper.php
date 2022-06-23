<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Helper;

class BackendHelper
{
    public static function generateStagingTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    public static function generateTempDedupTableName(): string
    {
        return '__temp_DEDUP_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    public static function generateRandomExportPrefix(): string
    {
        return str_replace('.', '_', uniqid('csvexport', true));
    }
}
