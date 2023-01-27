<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping\Bigquery;

use Keboola\TableBackendUtils\Escaping\QuoteInterface;

class BigqueryQuote implements QuoteInterface
{
    public static function quote(string $value): string
    {
        $q = "'";
        return $q . addslashes($value) . $q;
    }

    public static function quoteSingleIdentifier(string $str): string
    {
        $q = '`';
        return ($q . str_replace("$q", "$q$q", $str) . $q);
    }
}
