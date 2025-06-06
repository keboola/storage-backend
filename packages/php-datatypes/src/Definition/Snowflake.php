<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

class Snowflake extends Common
{
    public const TYPES_WITH_COMPLEX_LENGTH = [
        self::TYPE_NUMBER,
        self::TYPE_DECIMAL,
        self::TYPE_DEC,
        self::TYPE_NUMERIC,
    ];
    public const METADATA_BACKEND = 'snowflake';
    public const TYPE_NUMBER = 'NUMBER';
    public const TYPE_DEC = 'DEC';
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
    public const TYPE_CHAR_VARYING = 'CHAR VARYING';
    public const TYPE_CHARACTER_VARYING = 'CHARACTER VARYING';
    public const TYPE_NCHAR_VARYING = 'NCHAR VARYING';
    public const TYPE_NCHAR = 'NCHAR';
    public const TYPE_NVARCHAR = 'NVARCHAR';
    public const TYPE_NVARCHAR2 = 'NVARCHAR2';
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
    public const TYPE_OBJECT = 'OBJECT';
    public const TYPE_ARRAY = 'ARRAY';
    public const TYPE_GEOGRAPHY = 'GEOGRAPHY';
    public const TYPE_GEOMETRY = 'GEOMETRY';
    public const TYPE_VECTOR = 'VECTOR';
    public const TYPES = [
        self::TYPE_NUMBER,
        self::TYPE_DEC,
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
        self::TYPE_CHAR_VARYING,
        self::TYPE_CHARACTER_VARYING,
        self::TYPE_STRING,
        self::TYPE_TEXT,
        self::TYPE_NCHAR_VARYING,
        self::TYPE_NCHAR,
        self::TYPE_NVARCHAR,
        self::TYPE_NVARCHAR2,
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
        self::TYPE_OBJECT,
        self::TYPE_ARRAY,
        self::TYPE_GEOGRAPHY,
        self::TYPE_GEOMETRY,
        self::TYPE_VECTOR,
    ];
    public const DEFAULT_VARCHAR_LENGTH = 16777216;
    public const MAX_VARCHAR_LENGTH = 134217728;
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
     * @return array{
     *     character_maximum?:string|int|null,
     *     numeric_precision?:string|int|null,
     *     numeric_scale?:string|int|null
     * }
     */
    public function getArrayFromLength(): array
    {
        if ($this->isTypeWithComplexLength()) {
            if ($this->getLength() === null || $this->getLength() === '') {
                $parsed = [];
            } else {
                $parsed = array_map(intval(...), explode(',', (string) $this->getLength()));
            }
            $parsed = $parsed + [38, 0];
            return ['numeric_precision' => $parsed[0], 'numeric_scale' => $parsed[1]];
        }
        return ['character_maximum' => $this->getLength()];
    }

    /**
     * @phpstan-assert-if-true array{
     *      numeric_precision:string,
     *      numeric_scale:string
     * } $this->getArrayFromLength()
     * @phpstan-assert-if-false array{
     *      character_maximum:string
     * } $this->getArrayFromLength()
     */
    public function isTypeWithComplexLength(): bool
    {
        return in_array($this->getType(), self::TYPES_WITH_COMPLEX_LENGTH, true);
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
            case self::TYPE_DEC:
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
            case self::TYPE_CHAR_VARYING:
            case self::TYPE_CHARACTER_VARYING:
            case self::TYPE_STRING:
            case self::TYPE_TEXT:
            case self::TYPE_NCHAR_VARYING:
            case self::TYPE_NCHAR:
            case self::TYPE_NVARCHAR:
            case self::TYPE_NVARCHAR2:
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
            case self::TYPE_VECTOR:
                $valid = false;
                if ($length === null || !is_string($length)) {
                    break;
                }
                /** matches:
                 * TYPE - INT|FLOAT - case insensitive
                 * ,
                 * any white space zero or infinite times
                 * any digit with 0 to 4 places
                 */
                if (preg_match('/^(?<TYPE>INT|FLOAT),[^\S\r\n]*(?<DIM>[\d]{1,4})$/i', $length, $matches)) {
                    $dimension = (int) $matches['DIM'];
                    if ($dimension > 0 && $dimension <= 4096) {
                        $valid = true;
                    }
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
        $type = strtoupper($this->type);
        if ($type === self::TYPE_NUMBER && $this->length === '38,0') {
            return BaseType::INTEGER;
        }
        switch ($type) {
            case self::TYPE_INT:
            case self::TYPE_INTEGER:
            case self::TYPE_BIGINT:
            case self::TYPE_SMALLINT:
            case self::TYPE_TINYINT:
            case self::TYPE_BYTEINT:
                return BaseType::INTEGER;
            case self::TYPE_NUMBER:
            case self::TYPE_DECIMAL:
            case self::TYPE_DEC:
            case self::TYPE_NUMERIC:
                return BaseType::NUMERIC;
            case self::TYPE_FLOAT:
            case self::TYPE_FLOAT4:
            case self::TYPE_FLOAT8:
            case self::TYPE_DOUBLE:
            case self::TYPE_DOUBLE_PRECISION:
            case self::TYPE_REAL:
                return BaseType::FLOAT;
            case self::TYPE_BOOLEAN:
                return BaseType::BOOLEAN;
            case self::TYPE_DATE:
                return BaseType::DATE;
            case self::TYPE_DATETIME:
            case self::TYPE_TIMESTAMP:
            case self::TYPE_TIMESTAMP_NTZ:
            case self::TYPE_TIMESTAMP_LTZ:
            case self::TYPE_TIMESTAMP_TZ:
                return BaseType::TIMESTAMP;
            default:
                return BaseType::STRING;
        }
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

    public function getBackendBasetype(): string
    {
        return match (strtoupper($this->type)) {
            self::TYPE_NVARCHAR => self::TYPE_VARCHAR,
            self::TYPE_NVARCHAR2 => self::TYPE_VARCHAR,
            self::TYPE_CHAR => self::TYPE_VARCHAR,
            self::TYPE_CHARACTER => self::TYPE_VARCHAR,
            self::TYPE_CHAR_VARYING => self::TYPE_VARCHAR,
            self::TYPE_CHARACTER_VARYING => self::TYPE_VARCHAR,
            self::TYPE_NCHAR_VARYING => self::TYPE_VARCHAR,
            self::TYPE_NCHAR => self::TYPE_VARCHAR,
            self::TYPE_STRING => self::TYPE_VARCHAR,
            self::TYPE_TEXT => self::TYPE_VARCHAR,

            self::TYPE_VARBINARY => self::TYPE_BINARY,

            self::TYPE_DEC => self::TYPE_NUMBER,
            self::TYPE_DECIMAL => self::TYPE_NUMBER,
            self::TYPE_NUMERIC => self::TYPE_NUMBER,
            self::TYPE_INT => self::TYPE_NUMBER,
            self::TYPE_INTEGER => self::TYPE_NUMBER,
            self::TYPE_BIGINT => self::TYPE_NUMBER,
            self::TYPE_SMALLINT => self::TYPE_NUMBER,
            self::TYPE_TINYINT => self::TYPE_NUMBER,
            self::TYPE_BYTEINT => self::TYPE_NUMBER,

            self::TYPE_FLOAT => self::TYPE_FLOAT,
            self::TYPE_FLOAT4 => self::TYPE_FLOAT,
            self::TYPE_FLOAT8 => self::TYPE_FLOAT,
            self::TYPE_DOUBLE => self::TYPE_FLOAT,
            self::TYPE_DOUBLE_PRECISION => self::TYPE_FLOAT,
            self::TYPE_REAL => self::TYPE_FLOAT,

            self::TYPE_DATETIME => self::TYPE_TIMESTAMP_NTZ,
            default => $this->type
        };
    }

    public static function getDefinitionForBasetype(string $basetype): self
    {
        $basetype = strtoupper($basetype);

        if (!BaseType::isValid($basetype)) {
            throw new InvalidTypeException(sprintf('Base type "%s" is not valid.', $basetype));
        }

        return match ($basetype) {
            BaseType::BOOLEAN => new self(self::TYPE_BOOLEAN),
            BaseType::DATE => new self(self::TYPE_DATE),
            BaseType::FLOAT => new self(self::TYPE_FLOAT),
            BaseType::INTEGER => new self(self::TYPE_INTEGER),
            BaseType::NUMERIC => new self(self::TYPE_NUMERIC, ['length' => '38,9']),
            BaseType::STRING => new self(self::TYPE_VARCHAR),
            BaseType::TIMESTAMP => new self(self::TYPE_TIMESTAMP),
            default => throw new LogicException(sprintf('Definition for base type "%s" is missing.', $basetype))
        };
    }
}
