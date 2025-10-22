<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

class Sqlite extends Common
{
    public const TYPE_INTEGER = 'INTEGER';
    public const TYPE_REAL = 'REAL';
    public const TYPE_TEXT = 'TEXT';
    public const TYPE_BLOB = 'BLOB';
    public const TYPE_NUMERIC = 'NUMERIC';

    public const TYPES = [
        self::TYPE_INTEGER,
        self::TYPE_REAL,
        self::TYPE_TEXT,
        self::TYPE_BLOB,
        self::TYPE_NUMERIC,
    ];

    /**
     * @param array{
     *     length?:string|null,
     *     nullable?:bool,
     *     default?:string|null
     * } $options
     * @throws InvalidOptionException
     */
    public function __construct(string $type, array $options = [])
    {
        $this->validateType($type);
        $this->validateLength($type, $options['length'] ?? null);
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }
        parent::__construct($type, $options);
    }

    public function getSQLDefinition(): string
    {
        $definition = $this->getType();
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
        if ($length === null || $length === '') {
            return;
        }

        $valid = true;
        switch (strtoupper($type)) {
            case self::TYPE_TEXT:
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_NUMERIC:
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
                if ((int) $parts[0] <= 0) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && (int) $parts[1] > (int) $parts[0]) {
                    $valid = false;
                    break;
                }
                break;
            default:
                $valid = false;
                break;
        }

        if (!$valid) {
            throw new InvalidLengthException("'{$length}' is not valid length for {$type}");
        }
    }

    public function getBasetype(): string
    {
        switch (strtoupper($this->type)) {
            case self::TYPE_INTEGER:
                $basetype = BaseType::INTEGER;
                break;
            case self::TYPE_NUMERIC:
                $basetype = BaseType::NUMERIC;
                break;
            case self::TYPE_REAL:
                $basetype = BaseType::FLOAT;
                break;
            case self::TYPE_TEXT:
                $basetype = BaseType::STRING;
                break;
            case self::TYPE_BLOB:
                $basetype = BaseType::STRING;
                break;
            default:
                $basetype = BaseType::STRING;
                break;
        }
        return $basetype;
    }

    public static function getTypeByBasetype(string $basetype): string
    {
        switch ($basetype) {
            case BaseType::INTEGER:
                return self::TYPE_INTEGER;
            case BaseType::NUMERIC:
                return self::TYPE_NUMERIC;
            case BaseType::FLOAT:
                return self::TYPE_REAL;
            case BaseType::STRING:
                return self::TYPE_TEXT;
            case BaseType::DATE:
            case BaseType::TIMESTAMP:
                return self::TYPE_TEXT;
            case BaseType::BOOLEAN:
                return self::TYPE_INTEGER;
            default:
                return self::TYPE_TEXT;
        }
    }

    public static function getDefinitionForBasetype(string $basetype): DefinitionInterface
    {
        return new self(self::getTypeByBasetype($basetype));
    }
}
