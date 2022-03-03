<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

final class StringUtils
{
    public static function shuffle(string $string): string
    {
        $randomizedString = '';
        while ($string !== '') {
            $index = random_int(0, strlen($string) - 1);
            $randomizedString .= $string[$index];
            /** @var string $string */
            $string = substr_replace($string, '', $index, 1);
        }

        return $randomizedString;
    }
}
