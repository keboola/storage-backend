<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\Helper;

final class QuoteHelper
{
    public static function quoteValue(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }
}
