<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Exasol\Helper;

final class BackendHelper
{
    public static function generateTempTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    public static function generateTempDedupTableName(): string
    {
        return '__temp_DEDUP_' . str_replace('.', '_', uniqid('csvimport', true));
    }
}
