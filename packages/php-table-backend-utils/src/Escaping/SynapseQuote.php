<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping;

class SynapseQuote implements QuoteInterface
{
    public static function quote(string $value): string
    {
        return "'" . str_replace("'", "''", $value) . "'";
    }

    public static function quoteSingleIdentifier(string $str): string
    {
        return '[' . str_replace(']', ']]', $str) . ']';
    }
}
