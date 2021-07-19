<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping\Teradata;

use Keboola\TableBackendUtils\Escaping\QuoteInterface;

class TeradataQuote implements QuoteInterface
{
    /**
     * TD quotes strings by single quote
     * uses '' for escaping
     *
     * @param string $value
     * @return string
     */
    public static function quote(string $value): string
    {
        return "'" . str_replace("'", "''", $value) . "'";
    }

    /**
     * TD quotes strings by double quote
     * uses "" for escaping
     * https://docs.teradata.com/r/j4dzSri4DSHlIGdmgL_EvQ/xpojqfx2HVgFFhrFt_hCBA
     * @param string $str
     * @return string
     */
    public static function quoteSingleIdentifier(string $str): string
    {
        return '"' . str_replace('"', '""', $str) . '"';
    }
}
