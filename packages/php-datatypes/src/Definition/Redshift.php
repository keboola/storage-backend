<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidCompressionException;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

class Redshift extends Common
{
    public const SMALLINT = 'SMALLINT';
    public const INT2 = 'INT2';
    public const INTEGER = 'INTEGER';
    public const INT = 'INT';
    public const INT4 = 'INT4';
    public const BIGINT = 'BIGINT';
    public const INT8 = 'INT8';
    public const DECIMAL = 'DECIMAL';
    public const NUMERIC = 'NUMERIC';
    public const REAL = 'REAL';
    public const FLOAT4 = 'FLOAT4';
    public const DOUBLE_PRECISION = 'DOUBLE PRECISION';
    public const FLOAT8 = 'FLOAT8';
    public const FLOAT = 'FLOAT';
    public const BOOLEAN = 'BOOLEAN';
    public const BOOL = 'BOOL';
    public const CHAR = 'CHAR';
    public const CHARACTER = 'CHARACTER';
    public const NCHAR = 'NCHAR';
    public const BPCHAR = 'BPCHAR';
    public const VARCHAR = 'VARCHAR';
    public const CHARACTER_VARYING = 'CHARACTER VARYING';
    public const NVARCHAR = 'NVARCHAR';
    public const TEXT = 'TEXT';
    public const DATE = 'DATE';
    public const TIMESTAMP = 'TIMESTAMP';
    public const TIMESTAMP_WITHOUT_TIME_ZONE = 'TIMESTAMP WITHOUT TIME ZONE';
    public const TIMESTAMPTZ = 'TIMESTAMPTZ';
    public const TIMESTAMP_WITH_TIME_ZONE = 'TIMESTAMP WITH TIME ZONE';

    public const TYPES = [
        self::SMALLINT, self::INT2, self::INTEGER, self::INT, self::INT4, self::BIGINT, self::INT8,
        self::DECIMAL, self::NUMERIC,
        self::REAL, self::FLOAT4, self::DOUBLE_PRECISION, self::FLOAT8, self::FLOAT,
        self::BOOLEAN, self::BOOL,
        self::CHAR, self::CHARACTER, self::NCHAR, self::BPCHAR,
        self::VARCHAR, self::CHARACTER_VARYING, self::NVARCHAR, self::TEXT,
        self::DATE,
        self::TIMESTAMP, self::TIMESTAMP_WITHOUT_TIME_ZONE,
        self::TIMESTAMPTZ, self::TIMESTAMP_WITH_TIME_ZONE,
    ];

    protected ?string $compression = null;

    /**
     * Redshift constructor.
     *
     * @param array{length?:string|null, nullable?:bool, default?:string|null, compression?:string|null} $options
     * @throws InvalidOptionException
     */
    public function __construct(string $type, array $options = [])
    {
        $this->validateType($type);
        $options['length'] = $this->processLength($options);
        $this->validateLength($type, $options['length']);

        if (isset($options['compression'])) {
            $this->validateCompression($type, $options['compression']);
            $this->compression = $options['compression'];
        }
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default', 'compression']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }
        parent::__construct($type, $options);
    }

    public function getCompression(): ?string
    {
        return $this->compression;
    }

    public function getSQLDefinition(): string
    {
        $definition =  $this->getType();
        if ($this->getLength() && $this->getLength() !== '') {
            $definition .= '(' . $this->getLength() . ')';
        }
        if (!$this->isNullable()) {
            $definition .= ' NOT NULL';
        }
        if ($this->getCompression() && $this->getCompression() !== '') {
            $definition .= ' ENCODE ' . $this->getCompression();
        }
        return $definition;
    }

    /**
     * @return array{type:string,length:string|null,nullable:bool,compression:string|null}
     */
    public function toArray(): array
    {
        return [
            'type' => $this->getType(),
            'length' => $this->getLength(),
            'nullable' => $this->isNullable(),
            'compression' => $this->getCompression(),
        ];
    }

    /**
     * @param array{length?:string|array|null} $options
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
            case self::DECIMAL:
            case self::NUMERIC:
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
                if ((int) $parts[0] <= 0 || (int) $parts[0] > 37) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int) $parts[1] > 37 || (int) $parts[1] > (int) $parts[0])) {
                    $valid = false;
                    break;
                }
                break;
            case self::VARCHAR:
            case self::CHARACTER_VARYING:
            case self::TEXT:
            case self::NVARCHAR:
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 65535) {
                    $valid = false;
                    break;
                }
                break;
            case self::CHAR:
            case self::CHARACTER:
            case self::NCHAR:
            case self::BPCHAR:
                if (is_null($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 4096) {
                    $valid = false;
                    break;
                }
                break;
            case self::TIMESTAMP:
            case self::TIMESTAMP_WITHOUT_TIME_ZONE:
            case self::TIMESTAMPTZ:
            case self::TIMESTAMP_WITH_TIME_ZONE:
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 11) {
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

    /**
     * @throws InvalidCompressionException
     */
    private function validateCompression(string $type, string $compression): void
    {
        $valid = true;
        $type = strtoupper($type);
        switch (strtoupper($compression)) {
            case 'RAW':
            case 'ZSTD':
            case 'RUNLENGTH':
            case null:
            case '':
                break;
            case 'BYTEDICT':
                if (in_array($type, [self::BOOLEAN, self::BOOL])) {
                    $valid = false;
                }
                break;
            case 'DELTA':
                if (!in_array($type, [
                    self::SMALLINT,
                    self::INT2,
                    self::INT,
                    self::INTEGER,
                    self::INT4,
                    self::BIGINT,
                    self::INT8,
                    self::DATE,
                    self::TIMESTAMP,
                    self::TIMESTAMP_WITHOUT_TIME_ZONE,
                    self::TIMESTAMPTZ,
                    self::TIMESTAMP_WITH_TIME_ZONE,
                    self::DECIMAL,
                    self::NUMERIC,
                ])) {
                    $valid = false;
                }
                break;
            case 'DELTA32K':
                if (!in_array($type, [
                    self::INT,
                    self::INTEGER,
                    self::INT4,
                    self::BIGINT,
                    self::INT8,
                    self::DATE,
                    self::TIMESTAMP,
                    self::TIMESTAMP_WITHOUT_TIME_ZONE,
                    self::TIMESTAMPTZ,
                    self::TIMESTAMP_WITH_TIME_ZONE,
                    self::DECIMAL,
                    self::NUMERIC,
                ])) {
                    $valid = false;
                }
                break;
            case 'LZO':
                if (in_array($type, [
                    self::BOOLEAN,
                    self::BOOL,
                    self::REAL,
                    self::FLOAT4,
                    self::DOUBLE_PRECISION,
                    self::FLOAT8,
                    self::FLOAT,
                ])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY8':
                if (!in_array($type, [
                    self::SMALLINT,
                    self::INT2,
                    self::INT,
                    self::INTEGER,
                    self::INT4,
                    self::BIGINT,
                    self::INT8,
                    self::DECIMAL,
                    self::NUMERIC,
                ])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY16':
                if (!in_array($type, [
                    self::INT,
                    self::INTEGER,
                    self::INT4,
                    self::BIGINT,
                    self::INT8,
                    self::DECIMAL,
                    self::NUMERIC,
                ])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY32':
                if (!in_array($type, [self::BIGINT, self::INT8, self::DECIMAL, self::NUMERIC])) {
                    $valid = false;
                }
                break;
            case 'TEXT255':
            case 'TEXT32K':
                if (!in_array($type, [self::VARCHAR, self::CHARACTER_VARYING, self::NVARCHAR, self::TEXT])) {
                    $valid = false;
                }
                break;
            default:
                $valid = false;
                break;
        }
        if (!$valid) {
            throw new InvalidCompressionException("'{$compression}' is not valid compression for {$type}");
        }
    }

    public function getBasetype(): string
    {
        switch (strtoupper($this->type)) {
            case self::SMALLINT:
            case self::INT2:
            case self::INTEGER:
            case self::INT:
            case self::INT4:
            case self::BIGINT:
            case self::INT8:
                $basetype = BaseType::INTEGER;
                break;
            case self::DECIMAL:
            case self::NUMERIC:
                $basetype = BaseType::NUMERIC;
                break;
            case self::REAL:
            case self::FLOAT4:
            case self::DOUBLE_PRECISION:
            case self::FLOAT8:
            case self::FLOAT:
                $basetype = BaseType::FLOAT;
                break;
            case self::BOOLEAN:
            case self::BOOL:
                $basetype = BaseType::BOOLEAN;
                break;
            case self::DATE:
                $basetype = BaseType::DATE;
                break;
            case self::TIMESTAMP:
            case self::TIMESTAMP_WITHOUT_TIME_ZONE:
            case self::TIMESTAMPTZ:
            case self::TIMESTAMP_WITH_TIME_ZONE:
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
        throw new LogicException('Method is not implemented yet.');
    }

    /**
     * @return array<int, array{key:string,value:mixed}>
     */
    public function toMetadata(): array
    {
        $metadata = parent::toMetadata();
        if ($this->getCompression()) {
            $metadata[] = [
                'key' => Common::KBC_METADATA_KEY_COMPRESSION,
                'value' => $this->getCompression(),
            ];
        }
        return $metadata;
    }

    /**
     * {@inheritDoc}
     */
    public static function getTypesAllowedInFilters(): array
    {
        return [
            self::SMALLINT,
            self::INT2,
            self::INTEGER,
            self::INT,
            self::INT4,
            self::BIGINT,
            self::INT8,
            self::DECIMAL,
            self::NUMERIC,
            self::REAL,
            self::FLOAT4,
            self::DOUBLE_PRECISION,
            self::FLOAT8,
            self::FLOAT,
            self::BOOLEAN,
            self::BOOL,
            self::CHAR,
            self::CHARACTER,
            self::NCHAR,
            self::BPCHAR,
            self::VARCHAR,
            self::CHARACTER_VARYING,
            self::NVARCHAR,
            self::TEXT,
            self::DATE,
            self::TIMESTAMP,
            self::TIMESTAMP_WITHOUT_TIME_ZONE,
            self::TIMESTAMPTZ,
            self::TIMESTAMP_WITH_TIME_ZONE,
        ];
    }
}
