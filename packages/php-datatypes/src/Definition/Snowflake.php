<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

class Snowflake extends Common
{
    public const METADATA_BACKEND = 'snowflake';
    public const TYPE_NUMBER = 'NUMBER';
    public const TYPE_DECIMAL = 'DECIMAL';
    public const TYPE_NUMERIC = 'NUMERIC';
    public const TYPE_INT = 'INT';
    public const TYPE_INTEGER = 'INTEGER';
    public const TYPE_BIGINT = 'BIGINT';
    public const TYPE_SMALLINT = 'SMALLINT';
    public const TYPE_TINYINT = 'TINYINT';
    public const TYPE_BYTEINT = 'BYTEINT';
    public const TYPE_FLOAT = 'FLOAT';
    public const TYPE_FLOAT4 = 'FLOAT4';
    public const TYPE_FLOAT8 = 'FLOAT8';
    public const TYPE_DOUBLE = 'DOUBLE';
    public const TYPE_DOUBLE_PRECISION = 'DOUBLE PRECISION';
    public const TYPE_REAL = 'REAL';
    public const TYPE_VARCHAR = 'VARCHAR';
    public const TYPE_CHAR = 'CHAR';
    public const TYPE_CHARACTER = 'CHARACTER';
    public const TYPE_STRING = 'STRING';
    public const TYPE_TEXT = 'TEXT';
    public const TYPE_BOOLEAN = 'BOOLEAN';
    public const TYPE_DATE = 'DATE';
    public const TYPE_DATETIME = 'DATETIME';
    public const TYPE_TIME = 'TIME';
    public const TYPE_TIMESTAMP = 'TIMESTAMP';
    public const TYPE_TIMESTAMP_NTZ = 'TIMESTAMP_NTZ';
    public const TYPE_TIMESTAMP_LTZ = 'TIMESTAMP_LTZ';
    public const TYPE_TIMESTAMP_TZ = 'TIMESTAMP_TZ';
    public const TYPE_VARIANT = 'VARIANT';
    public const TYPE_BINARY = 'BINARY';
    public const TYPE_VARBINARY = 'VARBINARY';
    public const TYPES = [
        self::TYPE_NUMBER,
        self::TYPE_DECIMAL,
        self::TYPE_NUMERIC,
        self::TYPE_INT,
        self::TYPE_INTEGER,
        self::TYPE_BIGINT,
        self::TYPE_SMALLINT,
        self::TYPE_TINYINT,
        self::TYPE_BYTEINT,
        self::TYPE_FLOAT,
        self::TYPE_FLOAT4,
        self::TYPE_FLOAT8,
        self::TYPE_DOUBLE,
        self::TYPE_DOUBLE_PRECISION,
        self::TYPE_REAL,
        self::TYPE_VARCHAR,
        self::TYPE_CHAR,
        self::TYPE_CHARACTER,
        self::TYPE_STRING,
        self::TYPE_TEXT,
        self::TYPE_BOOLEAN,
        self::TYPE_DATE,
        self::TYPE_DATETIME,
        self::TYPE_TIME,
        self::TYPE_TIMESTAMP,
        self::TYPE_TIMESTAMP_NTZ,
        self::TYPE_TIMESTAMP_LTZ,
        self::TYPE_TIMESTAMP_TZ,
        self::TYPE_VARIANT,
        self::TYPE_BINARY,
        self::TYPE_VARBINARY,
    ];

    public const MAX_VARCHAR_LENGTH = 16777216;
    public const MAX_VARBINARY_LENGTH = 8388608;

    /**
     * Snowflake constructor.
     *
     * @param array{
     *     length?:string|null|array,
     *     nullable?:bool,
     *     default?:string|null
     * } $options
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function __construct(string $type, array $options = [])
    {
        $this->validateType($type);
        $options['length'] = $this->processLength($options);
        $this->validateLength($type, $options['length']);
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }
        if (array_key_exists('default', $options) && $options['default'] === '') {
            unset($options['default']);
        }
        parent::__construct($type, $options);
    }


    public function getTypeOnlySQLDefinition(): string
    {
        $out = $this->getType();
        $length = $this->getLength();
        if ($length !== null && $length !== '') {
            $out .= ' (' . $length . ')';
        }
        return $out;
    }

    public function getSQLDefinition(): string
    {
        $definition = $this->getTypeOnlySQLDefinition();
        if (!$this->isNullable()) {
            $definition .= ' NOT NULL';
        }
        if ($this->getDefault() !== null) {
            $definition .= ' DEFAULT ' . $this->getDefault();
        }
        return $definition;
    }

    /**
     * @return array{type:string,length:string|null,nullable:bool}
     */
    public function toArray(): array
    {
        return [
            'type' => $this->getType(),
            'length' => $this->getLength(),
            'nullable' => $this->isNullable(),
        ];
    }

    /**
     * @param array{length?: string|null|array} $options
     * @throws InvalidOptionException
     */
    private function processLength(array $options): ?string
    {
        if (!isset($options['length'])) {
            return null;
        }
        if (is_array($options['length'])) {
            return $this->getLengthFromArray($options['length']);
        }
        return (string) $options['length'];
    }

    /**
     * @param array{
     *     character_maximum?:string|int|null,
     *     numeric_precision?:string|int|null,
     *     numeric_scale?:string|int|null
     * } $lengthOptions
     * @throws InvalidOptionException
     */
    private function getLengthFromArray(array $lengthOptions): ?string
    {
        $expectedOptions = ['character_maximum', 'numeric_precision', 'numeric_scale'];
        $diff = array_diff(array_keys($lengthOptions), $expectedOptions);
        if ($diff !== []) {
            throw new InvalidOptionException(sprintf('Length option "%s" not supported', $diff[0]));
        }

        $characterMaximum = $lengthOptions['character_maximum'] ?? null;
        $numericPrecision = $lengthOptions['numeric_precision'] ?? null;
        $numericScale = $lengthOptions['numeric_scale'] ?? null;

        if (!is_null($characterMaximum)) {
            return (string) $characterMaximum;
        }
        if (!is_null($numericPrecision) && !is_null($numericScale)) {
            return $numericPrecision . ',' . $numericScale;
        }
        return $numericPrecision === null ? null : (string) $numericPrecision;
    }

    /**
     * @throws InvalidTypeException
     */
    private function validateType(string $type): void
    {
        if (!in_array(strtoupper($type), $this::TYPES)) {
            throw new InvalidTypeException("'{$type}' is not a valid type");
        }
    }

    /**
     * @param null|int|string $length
     * @throws InvalidLengthException
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    private function validateLength(string $type, $length = null): void
    {
        $valid = true;
        switch (strtoupper($type)) {
            case self::TYPE_NUMBER:
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                if (is_null($length) || $length === '') {
                    break;
                }
                $parts = explode(',', (string) $length);
                if (!in_array(count($parts), [1, 2])) {
                    $valid = false;
                    break;
                }
                if (!is_numeric($parts[0])) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && !is_numeric($parts[1])) {
                    $valid = false;
                    break;
                }
                if ((int) $parts[0] <= 0 || (int) $parts[0] > 38) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int) $parts[1] > 38 || (int) $parts[1] > (int) $parts[0])) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_VARCHAR:
            case self::TYPE_CHAR:
            case self::TYPE_CHARACTER:
            case self::TYPE_STRING:
            case self::TYPE_TEXT:
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > self::MAX_VARCHAR_LENGTH) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_TIME:
            case self::TYPE_DATETIME:
            case self::TYPE_TIMESTAMP:
            case self::TYPE_TIMESTAMP_NTZ:
            case self::TYPE_TIMESTAMP_LTZ:
            case self::TYPE_TIMESTAMP_TZ:
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 0 || (int) $length > 9) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_BINARY:
            case self::TYPE_VARBINARY:
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 1 || (int) $length > self::MAX_VARBINARY_LENGTH) {
                    $valid = false;
                    break;
                }
                break;
            default:
                if (!is_null($length) && $length !== '') {
                    $valid = false;
                    break;
                }
                break;
        }
        if (!$valid) {
            throw new InvalidLengthException("'{$length}' is not valid length for {$type}");
        }
    }

    public function getBasetype(): string
    {
        switch (strtoupper($this->type)) {
            case self::TYPE_INT:
            case self::TYPE_INTEGER:
            case self::TYPE_BIGINT:
            case self::TYPE_SMALLINT:
            case self::TYPE_TINYINT:
            case self::TYPE_BYTEINT:
                $basetype = BaseType::INTEGER;
                break;
            case self::TYPE_NUMBER:
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                $basetype = BaseType::NUMERIC;
                break;
            case self::TYPE_FLOAT:
            case self::TYPE_FLOAT4:
            case self::TYPE_FLOAT8:
            case self::TYPE_DOUBLE:
            case self::TYPE_DOUBLE_PRECISION:
            case self::TYPE_REAL:
                $basetype = BaseType::FLOAT;
                break;
            case self::TYPE_BOOLEAN:
                $basetype = BaseType::BOOLEAN;
                break;
            case self::TYPE_DATE:
                $basetype = BaseType::DATE;
                break;
            case self::TYPE_DATETIME:
            case self::TYPE_TIMESTAMP:
            case self::TYPE_TIMESTAMP_NTZ:
            case self::TYPE_TIMESTAMP_LTZ:
            case self::TYPE_TIMESTAMP_TZ:
                $basetype = BaseType::TIMESTAMP;
                break;
            default:
                $basetype = BaseType::STRING;
                break;
        }
        return $basetype;
    }

    public static function getTypeByBasetype(string $basetype): string
    {
        $basetype = strtoupper($basetype);

        if (!BaseType::isValid($basetype)) {
            throw new InvalidTypeException(sprintf('Base type "%s" is not valid.', $basetype));
        }

        switch ($basetype) {
            case BaseType::BOOLEAN:
                return self::TYPE_BOOLEAN;
            case BaseType::DATE:
                return self::TYPE_DATE;
            case BaseType::FLOAT:
                return self::TYPE_FLOAT;
            case BaseType::INTEGER:
                return self::TYPE_INTEGER;
            case BaseType::NUMERIC:
                return self::TYPE_NUMBER;
            case BaseType::STRING:
                return self::TYPE_VARCHAR;
            case BaseType::TIMESTAMP:
                return self::TYPE_TIMESTAMP;
        }

        throw new LogicException(sprintf('Definition for base type "%s" is missing.', $basetype));
    }
}
