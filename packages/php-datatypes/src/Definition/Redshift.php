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
    public const TYPES = [
        'SMALLINT', 'INT2', 'INTEGER', 'INT', 'INT4', 'BIGINT', 'INT8',
        'DECIMAL', 'NUMERIC',
        'REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'FLOAT',
        'BOOLEAN', 'BOOL',
        'CHAR', 'CHARACTER', 'NCHAR', 'BPCHAR',
        'VARCHAR', 'CHARACTER VARYING', 'NVARCHAR', 'TEXT',
        'DATE',
        'TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE',
        'TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE',
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
            case 'DECIMAL':
            case 'NUMERIC':
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
            case 'VARCHAR':
            case 'CHARACTER VARYING':
            case 'TEXT':
            case 'NVARCHAR':
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
            case 'CHAR':
            case 'CHARACTER':
            case 'NCHAR':
            case 'BPCHAR':
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
            case 'TIMESTAMP':
            case 'TIMESTAMP WITHOUT TIME ZONE':
            case 'TIMESTAMPTZ':
            case 'TIMESTAMP WITH TIME ZONE':
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
                if (in_array($type, ['BOOLEAN', 'BOOL'])) {
                    $valid = false;
                }
                break;
            case 'DELTA':
                if (!in_array($type, [
                    'SMALLINT',
                    'INT2',
                    'INT',
                    'INTEGER',
                    'INT4',
                    'BIGINT',
                    'INT8',
                    'DATE',
                    'TIMESTAMP',
                    'TIMESTAMP WITHOUT TIME ZONE',
                    'TIMESTAMPTZ',
                    'TIMESTAMP WITH TIMEZONE',
                    'DECIMAL',
                    'NUMERIC',
                ])) {
                    $valid = false;
                }
                break;
            case 'DELTA32K':
                if (!in_array($type, [
                    'INT',
                    'INTEGER',
                    'INT4',
                    'BIGINT',
                    'INT8',
                    'DATE',
                    'TIMESTAMP',
                    'TIMESTAMP WITHOUT TIME ZONE',
                    'TIMESTAMPTZ',
                    'TIMESTAMP WITH TIMEZONE',
                    'DECIMAL',
                    'NUMERIC',
                ])) {
                    $valid = false;
                }
                break;
            case 'LZO':
                if (in_array($type, ['BOOLEAN', 'BOOL', 'REAL', 'FLOAT4', 'DOUBLE PRECISION', 'FLOAT8', 'FLOAT'])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY8':
                if (!in_array($type, [
                    'SMALLINT',
                    'INT2',
                    'INT',
                    'INTEGER',
                    'INT4',
                    'BIGINT',
                    'INT8',
                    'DECIMAL',
                    'NUMERIC',
                ])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY16':
                if (!in_array($type, ['INT', 'INTEGER', 'INT4', 'BIGINT', 'INT8', 'DECIMAL', 'NUMERIC'])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY32':
                if (!in_array($type, ['BIGINT', 'INT8', 'DECIMAL', 'NUMERIC'])) {
                    $valid = false;
                }
                break;
            case 'TEXT255':
            case 'TEXT32K':
                if (!in_array($type, ['VARCHAR', 'CHARACTER VARYING', 'NVARCHAR', 'TEXT'])) {
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
            case 'SMALLINT':
            case 'INT2':
            case 'INTEGER':
            case 'INT':
            case 'INT4':
            case 'BIGINT':
            case 'INT8':
                $basetype = BaseType::INTEGER;
                break;
            case 'DECIMAL':
            case 'NUMERIC':
                $basetype = BaseType::NUMERIC;
                break;
            case 'REAL':
            case 'FLOAT4':
            case 'DOUBLE PRECISION':
            case 'FLOAT8':
            case 'FLOAT':
                $basetype = BaseType::FLOAT;
                break;
            case 'BOOLEAN':
            case 'BOOL':
                $basetype = BaseType::BOOLEAN;
                break;
            case 'DATE':
                $basetype = BaseType::DATE;
                break;
            case 'TIMESTAMP':
            case 'TIMESTAMP WITHOUT TIME ZONE':
            case 'TIMESTAMPTZ':
            case 'TIMESTAMP WITH TIME ZONE':
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
}
