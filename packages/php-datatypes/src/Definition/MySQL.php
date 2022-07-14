<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

class MySQL extends Common
{
    public const TYPES = [
        'INTEGER', 'INT',
        'TINYINT', 'SMALLINT', 'MEDIUMINT', 'BIGINT',
        'DECIMAL', 'DEC', 'FIXED', 'NUMERIC',
        'FLOAT', 'DOUBLE PRECISION', 'REAL', 'DOUBLE',
        'BIT',
        'DATE',
        'TIME',
        'DATETIME',
        'TIMESTAMP',
        'YEAR',
        'CHAR',
        'VARCHAR',
        'BLOB',
        'TEXT',
    ];

    /**
     * Snowflake constructor.
     *
     * @param array{
     *     length?:string|null|array,
     *     nullable?:bool,
     *     default?:string|null
     * } $options
     * @throws InvalidOptionException
     */
    public function __construct(string $type, array $options = [])
    {
        $this->validateType($type);
        $options['length'] = $this->processLength($options);
        $this->validateLength($type, $options['length'] ?? null);
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }
        parent::__construct($type, $options);
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
            case 'CHAR':
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 255) {
                    $valid = false;
                    break;
                }
                break;
            case 'VARCHAR':
                if (is_null($length) || $length === '' || !is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 4_294_967_295) {
                    $valid = false;
                    break;
                }
                break;

            case 'INT':
            case 'INTEGER':
            case 'TINYINT':
            case 'SMALLINT':
            case 'MEDIUMINT':
            case 'BIGINT':
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 255) {
                    $valid = false;
                    break;
                }
                break;

            case 'DOUBLE PRECISION':
            case 'REAL':
            case 'DOUBLE':
            case 'FLOAT':
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
                if ((int) $parts[0] <= 0 || (int) $parts[0] > 255) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int) $parts[1] >= (int) $parts[0] || (int) $parts[1] > 30)) {
                    $valid = false;
                    break;
                }
                break;

            case 'DECIMAL':
            case 'DEC':
            case 'FIXED':
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
                if ((int) $parts[0] <= 0 || (int) $parts[0] > 65) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int) $parts[1] > (int) $parts[0] || (int) $parts[1] > 30)) {
                    $valid = false;
                    break;
                }
                break;

            case 'TIME':
                if (is_null($length) || $length === '') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 0 || (int) $length > 6) {
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
            case 'INT':
            case 'INTEGER':
            case 'BIGINT':
            case 'SMALLINT':
            case 'TINYINT':
            case 'MEDIUMINT':
                $basetype = BaseType::INTEGER;
                break;
            case 'NUMERIC':
            case 'DECIMAL':
            case 'DEC':
            case 'FIXED':
                $basetype = BaseType::NUMERIC;
                break;
            case 'FLOAT':
            case 'DOUBLE PRECISION':
            case 'REAL':
            case 'DOUBLE':
                $basetype = BaseType::FLOAT;
                break;
            case 'DATE':
                $basetype = BaseType::DATE;
                break;
            case 'DATETIME':
            case 'TIMESTAMP':
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
}
