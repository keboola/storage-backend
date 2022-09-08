<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Utils;

class StringCaseSensitivity
{
    public static function stringToLower(string $string): string
    {
        return strtolower($string);
    }

    public static function isEqualCaseInsensitive(string $a, string $b): bool
    {
        return strcasecmp($a, $b) === 0;
    }

    /**
     * @param string[] $arr
     * @return string[]
     */
    public static function arrayToLower(array $arr): array
    {
        return array_map(static fn(string $string) => strtolower($string), $arr);
    }

    /**
     * @param string[] $haystack
     */
    public static function isInArrayCaseInsensitive(string $needle, array $haystack): bool
    {
        return in_array(strtolower($needle), self::arrayToLower($haystack), true);
    }
}
