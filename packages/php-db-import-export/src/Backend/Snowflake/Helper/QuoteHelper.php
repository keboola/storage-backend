<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

final class QuoteHelper
{
    public static function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    public static function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }
}
