<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

final class Charset
{
    public const CHARSET_RANGE_DEFINITION_LOWERCASE = 'a-z';
    public const CHARSET_RANGE_DEFINITION_UPPERCASE = 'A-Z';
    public const CHARSET_RANGE_DEFINITION_NUMBERS = '0-9';

    public static function getCharlistFromRange(string $range, string $delimiter = ''): string
    {
        /** @var string $charlist */
        $charlist = preg_replace_callback('#.-.#', static function (array $m) use ($delimiter) {
            return implode($delimiter, range($m[0][0], $m[0][2]));
        }, $range);
        return $charlist;
    }
}
