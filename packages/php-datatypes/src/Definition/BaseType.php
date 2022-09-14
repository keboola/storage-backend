<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

class BaseType
{
    public const BOOLEAN = 'BOOLEAN';
    public const DATE = 'DATE';
    public const FLOAT = 'FLOAT';
    public const INTEGER = 'INTEGER';
    public const NUMERIC = 'NUMERIC';
    public const STRING = 'STRING';
    public const TIMESTAMP = 'TIMESTAMP';

    public const TYPES = [
        self::BOOLEAN,
        self::DATE,
        self::FLOAT,
        self::INTEGER,
        self::NUMERIC,
        self::STRING,
        self::TIMESTAMP,
    ];

    public static function isValid(string $basetype): bool
    {
        return array_key_exists($basetype, array_flip(self::TYPES));
    }
}
