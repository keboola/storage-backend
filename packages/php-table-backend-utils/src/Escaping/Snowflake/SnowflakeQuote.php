<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping\Snowflake;

use Keboola\TableBackendUtils\Escaping\QuoteInterface;

class SnowflakeQuote implements QuoteInterface
{
    public static function quote(string $value): string
    {
        $q = "'";
        return $q . addslashes($value) . $q;
    }

    public static function quoteSingleIdentifier(string $str): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $str) . $q);
    }

    /**
     * Takes array of database, schema (both optional) and table and converts them
     * to a quoted identifier "database"."schema"."table"
     *
     * @param string[] $parts
     */
    public static function createQuotedIdentifierFromParts(array $parts): string
    {
        return implode('.', array_map(
            function (string $part) {
                return self::quoteSingleIdentifier($part);
            },
            $parts
        ));
    }
}
