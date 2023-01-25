<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping\Exasol;

use Keboola\TableBackendUtils\Escaping\QuoteInterface;

class ExasolQuote implements QuoteInterface
{
    /**
     * Exasol quotes strings by single quote
     * uses '' for escaping
     */
    public static function quote(string $value): string
    {
        return "'" . str_replace("'", "''", $value) . "'";
    }

    /**
     * Exasol quotes identifiers by double quote
     *  - identifiers without "" are tranformed to UPPERCASE
     *  - qouted identifiers are kept in desired form
     * Query  | Exasol representation
     * ------------------------------
     *  AaA   | AAA
     *  "AaA" | AaA
     * uses "" for escaping
     */
    public static function quoteSingleIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }
}
