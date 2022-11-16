<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping;

class RedshiftQuote implements QuoteInterface
{
    public static function quote(string $value): string
    {
        $q = "'";
        return ($q . str_replace($q, $q . $q, $value) . $q);
    }

    public static function quoteSingleIdentifier(string $str): string
    {
        $q = '"';
        return ($q . str_replace($q, $q . $q, $str) . $q);
    }
}
