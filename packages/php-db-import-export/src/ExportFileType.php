<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

enum ExportFileType: int
{
    case CSV = 1;
    case PARQUET = 2;

    public function getFileExtension(): string
    {
        return match ($this) {
            self::CSV => 'csv',
            self::PARQUET => 'parquet',
        };
    }
}
