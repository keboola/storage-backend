<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

/**
 * Class Oracle
 *
 * Datatypes reference: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html
 */
class Oracle extends Common
{
    // Numeric Types
    public const TYPE_NUMBER = 'NUMBER';
    public const TYPE_BINARY_FLOAT = 'BINARY_FLOAT';
    public const TYPE_BINARY_DOUBLE = 'BINARY_DOUBLE';

    // Date and Time
    public const TYPE_DATE = 'DATE';
    public const TYPE_TIMESTAMP = 'TIMESTAMP';
    public const TYPE_TIMESTAMP_LTZ = 'TIMESTAMP WITH LOCAL TIME ZONE';

    // Strings and Characters
    public const TYPE_VARCHAR2 = 'VARCHAR2';
    public const TYPE_CHAR = 'CHAR';
    public const TYPE_CLOB = 'CLOB';

    // Other Oracle types (for simplicity, this is limited)
    public const TYPE_RAW = 'RAW';
    public const TYPE_BLOB = 'BLOB';

    public const TYPES = [
        self::TYPE_NUMBER,
        self::TYPE_BINARY_FLOAT,
        self::TYPE_BINARY_DOUBLE,
        self::TYPE_DATE,
        self::TYPE_TIMESTAMP,
        self::TYPE_TIMESTAMP_LTZ,
        self::TYPE_VARCHAR2,
        self::TYPE_CHAR,
        self::TYPE_CLOB,
        self::TYPE_RAW,
        self::TYPE_BLOB,
    ];

    /**
     * @param array{length?:string|null, nullable?:bool, default?:string|null} $options
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     * @throws InvalidLengthException
     */
    public function __construct(string $type, array $options = [])
    {
        $this->validateType($type);
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }

        if (array_key_exists('default', $options) && $options['default'] === '') {
            unset($options['default']);
        }
        parent::__construct($type, $options);
    }

    public function getSQLDefinition(): string
    {
        $definition = $this->type;

        if ($this->length !== null && in_array($this->type, [self::TYPE_VARCHAR2, self::TYPE_CHAR])) {
            $definition .= '(' . $this->length . ')';
        }

        if (!$this->nullable) {
            $definition .= ' NOT NULL';
        }

        if ($this->default !== null) {
            $definition .= ' DEFAULT ' . $this->default;
        }

        return $definition;
    }

    /**
     * @throws \Keboola\Datatype\Definition\Exception\InvalidTypeException
     */
    public function getBasetype(): string
    {
        return match ($this->type) {
            self::TYPE_VARCHAR2, self::TYPE_CHAR, self::TYPE_CLOB, self::TYPE_RAW, self::TYPE_BLOB => BaseType::STRING,
            self::TYPE_NUMBER, self::TYPE_BINARY_FLOAT, self::TYPE_BINARY_DOUBLE => BaseType::NUMERIC,
            // DATE in Oracle includes time, so it is mapped to TIMESTAMP
            self::TYPE_DATE, self::TYPE_TIMESTAMP, self::TYPE_TIMESTAMP_LTZ => BaseType::TIMESTAMP,
            default => throw new InvalidTypeException(sprintf('No base type mapped for type "%s"', $this->type)),
        };
    }

    /**
     * @return array{type:string, length:string|null, nullable:bool, compression?:mixed}
     */
    public function toArray(): array
    {
        return [
            'type' => $this->type,
            'length' => $this->length,
            'nullable' => $this->nullable,
            'default' => $this->default,
        ];
    }

    /**
     * @throws \Keboola\Datatype\Definition\Exception\InvalidTypeException
     */
    public static function getTypeByBasetype(string $basetype): string
    {
        return match ($basetype) {
            BaseType::STRING => self::TYPE_VARCHAR2,
            BaseType::NUMERIC, BaseType::FLOAT, BaseType::BOOLEAN, BaseType::INTEGER => self::TYPE_NUMBER,
            BaseType::TIMESTAMP, BaseType::DATE => self::TYPE_DATE,
            default => throw new InvalidTypeException(
                sprintf('No Oracle type mapped for base type "%s"', $basetype),
            ),
        };
    }

    /**
     * @throws InvalidTypeException
     */
    private function validateType(string $type): void
    {
        if (!in_array(strtoupper($type), self::TYPES, true)) {
            throw new InvalidTypeException(sprintf('"%s" is not a valid type', $type));
        }
    }
}
