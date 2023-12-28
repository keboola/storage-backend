<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

final class Password
{
    public const DEFAULT_SETS =
        self::SET_LOWERCASE |
        self::SET_UPPERCASE |
        self::SET_NUMBER;
    private const CHARSET_LIST_SYMBOLS = '!@#$%&*?/.+';
    // character which could be misread
    public const DEFAULT_EXCLUDED_CHARACTERS = '0O1Il';
    public const DEFAULT_PASSWORD_LENGTH = 32;
    public const SET_LOWERCASE = 1;
    public const SET_UPPERCASE = 2;
    public const SET_NUMBER = 4;
    public const SET_SPECIAL_CHARACTERS = 8;

    public static function generate(
        int $length = self::DEFAULT_PASSWORD_LENGTH,
        int $usedSets = self::DEFAULT_SETS,
        string $excludeChars = self::DEFAULT_EXCLUDED_CHARACTERS,
    ): string {

        $sets = [];
        if ($usedSets & self::SET_LOWERCASE) {
            $sets[] = Charset::getCharlistFromRange(Charset::CHARSET_RANGE_DEFINITION_LOWERCASE);
        }
        if ($usedSets & self::SET_UPPERCASE) {
            $sets[] = Charset::getCharlistFromRange(Charset::CHARSET_RANGE_DEFINITION_UPPERCASE);
        }
        if ($usedSets & self::SET_NUMBER) {
            $sets[] = Charset::getCharlistFromRange(Charset::CHARSET_RANGE_DEFINITION_NUMBERS);
        }
        if ($usedSets & self::SET_SPECIAL_CHARACTERS) {
            $sets[] = self::CHARSET_LIST_SYMBOLS;
        }

        $all = '';
        $password = '';
        // this ensures one character from each set
        foreach ($sets as $set) {
            // remove excluded characters
            $set = str_replace(str_split($excludeChars), '', $set);
            // pick one random character from each set
            $password .= $set[random_int(0, count(str_split($set)) - 1)];
            $all .= $set;
        }

        $all = str_split($all);
        while (strlen($password) < $length) {
            // randomize rest of password
            $password .= $all[random_int(0, count($all) - 1)];
        }

        // randomize crypto safe final password
        return StringUtils::shuffle($password);
    }
}
