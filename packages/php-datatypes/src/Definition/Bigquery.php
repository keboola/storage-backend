<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

/**
 * Class Bigquery
 *
 * https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
 *
 * @phpstan-type BigqueryTableFieldSchema array{
 *   name: string,
 *   type: string,
 *   mode?: 'NULLABLE'|'REQUIRED'|'REPEATED',
 *   fields?: array<mixed>,
 *   description?: string,
 *   maxLength?: string,
 *   precision?: string,
 *   scale?: string,
 *   roundingMode?: 'ROUNDING_MODE_UNSPECIFIED'|'ROUND_HALF_AWAY_FROM_ZERO'|'ROUND_HALF_EVEN',
 *   collation?: string,
 *   defaultValueExpression?: string
 *  }
 * structure returned by BQ REST API, fields is recursive but this is not supported by phpstan
 * Table schema docs: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema
 */
class Bigquery extends Common
{
    public const NUMERIC_LENGTH_CONST = 29;
    public const BIGNUMERIC_LENGTH_CONST = 38;

    public const TYPE_ARRAY = 'ARRAY';

    public const TYPE_BOOL = 'BOOL'; // NULL,TRUE,FALSE

    public const TYPE_BYTES = 'BYTES'; // BYTES(L) L is a positive INT64

    /* Datetime */
    public const TYPE_DATE = 'DATE';
    public const TYPE_DATETIME = 'DATETIME';
    public const TYPE_TIME = 'TIME';
    public const TYPE_TIMESTAMP = 'TIMESTAMP';

    public const TYPE_GEOGRAPHY = 'GEOGRAPHY';

    public const TYPE_INTERVAL = 'INTERVAL';

    public const TYPE_JSON = 'JSON';

    /* Numeric */
    public const TYPE_INT64 = 'INT64';
    // aliases for INT64
    public const TYPE_INT = 'INT';
    public const TYPE_SMALLINT = 'SMALLINT';
    public const TYPE_INTEGER = 'INTEGER';
    public const TYPE_BIGINT = 'BIGINT';
    public const TYPE_TINYINT = 'TINYINT';
    public const TYPE_BYTEINT = 'BYTEINT';

    public const TYPE_NUMERIC = 'NUMERIC'; // 0 ≤ S ≤ 9, max(1, S) ≤ P ≤ S + 29
    // alias for NUMERIC
    public const TYPE_DECIMAL = 'DECIMAL';

    public const TYPE_BIGNUMERIC = 'BIGNUMERIC'; // 0 ≤ S ≤ 38, max(1, S) ≤ P ≤ S + 38
    // alias for BIGNUMERIC
    public const TYPE_BIGDECIMAL = 'BIGDECIMAL';

    public const TYPE_FLOAT64 = 'FLOAT64';

    public const TYPE_STRING = 'STRING'; // STRING(L) L is a positive INT64 value

    public const TYPE_STRUCT = 'STRUCT';

    public const TYPES = [
        self::TYPE_ARRAY,
        self::TYPE_BOOL,
        self::TYPE_BYTES,
        self::TYPE_DATE,
        self::TYPE_DATETIME,
        self::TYPE_TIME,
        self::TYPE_TIMESTAMP,
        self::TYPE_GEOGRAPHY,
        self::TYPE_INTERVAL,
        self::TYPE_JSON,
        self::TYPE_INT64,
        self::TYPE_NUMERIC,
        self::TYPE_BIGNUMERIC,
        self::TYPE_FLOAT64,
        self::TYPE_STRING,
        self::TYPE_STRUCT,

        // aliases
        self::TYPE_INT,
        self::TYPE_SMALLINT,
        self::TYPE_INTEGER,
        self::TYPE_BIGINT,
        self::TYPE_TINYINT,
        self::TYPE_BYTEINT,
        self::TYPE_DECIMAL,
        self::TYPE_BIGDECIMAL,
    ];

    public const MAX_LENGTH = 9223372036854775807;

    /** @phpstan-var BigqueryTableFieldSchema|null */
    private ?array $fieldAsArray = null;

    /**
     * @phpstan-param array{
     *     length?:string|null,
     *     nullable?:bool,
     *     default?:string|null,
     *     fieldAsArray?:BigqueryTableFieldSchema
     * } $options
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function __construct(string $type, array $options = [])
    {
        $this->validateType($type);
        $this->validateLength($type, $options['length'] ?? null);

        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default', 'fieldAsArray']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }

        if (array_key_exists('default', $options) && $options['default'] === '') {
            unset($options['default']);
        }
        if (array_key_exists('fieldAsArray', $options)) {
            if (is_array($options['fieldAsArray'])) {
                $this->fieldAsArray = $options['fieldAsArray'];
            }
            unset($options['fieldAsArray']);
        }
        parent::__construct($type, $options);
    }

    public function getTypeOnlySQLDefinition(): string
    {
        $out = $this->getType();
        if (strtoupper($out) === self::TYPE_ARRAY || strtoupper($out) === self::TYPE_STRUCT) {
            $length = $this->getLength();
            if ($length !== null && $length !== '') {
                $out .= sprintf('<%s>', $length);
            }
        } else {
            $length = $this->getLength();
            if ($length !== null && $length !== '') {
                $out .= sprintf('(%s)', $length);
            }
        }
        return $out;
    }

    /**
     * @phpstan-return BigqueryTableFieldSchema|null
     */
    public function getFieldAsArray(): ?array
    {
        return $this->fieldAsArray;
    }

    public function getSQLDefinition(): string
    {
        $definition = $this->getTypeOnlySQLDefinition();

        if (strtoupper($this->getType()) === self::TYPE_ARRAY
        || strtoupper($this->getType()) === self::TYPE_STRUCT
        ) {
            return $definition;
        }
        if ($this->getDefault() !== null) {
            $definition .= ' DEFAULT ' . $this->getDefault();
        }
        if (!$this->isNullable()) {
            $definition .= ' NOT NULL';
        }
        return $definition;
    }

    public function getBasetype(): string
    {
        switch (strtoupper($this->type)) {
            case self::TYPE_INT64:
            case self::TYPE_INT:
            case self::TYPE_SMALLINT:
            case self::TYPE_INTEGER:
            case self::TYPE_BIGINT:
            case self::TYPE_TINYINT:
            case self::TYPE_BYTEINT:
                $basetype = BaseType::INTEGER;
                break;
            case self::TYPE_NUMERIC:
            case self::TYPE_DECIMAL:
            case self::TYPE_BIGNUMERIC:
            case self::TYPE_BIGDECIMAL:
                $basetype = BaseType::NUMERIC;
                break;
            case self::TYPE_FLOAT64:
                $basetype = BaseType::FLOAT;
                break;
            case self::TYPE_BOOL:
                $basetype = BaseType::BOOLEAN;
                break;
            case self::TYPE_DATE:
                $basetype = BaseType::DATE;
                break;
            case self::TYPE_DATETIME:
            case self::TYPE_TIME:
            case self::TYPE_TIMESTAMP:
                $basetype = BaseType::TIMESTAMP;
                break;
            default:
                $basetype = BaseType::STRING;
                break;
        }
        return $basetype;
    }

    /**
     * @return array{type:string,length:string|null,default:string|null,nullable:bool}
     */
    public function toArray(): array
    {
        return [
            'type' => $this->getType(),
            'length' => $this->getLength(),
            'default' => $this->getDefault(),
            'nullable' => $this->isNullable(),
        ];
    }

    public static function getTypeByBasetype(string $basetype): string
    {
        $basetype = strtoupper($basetype);

        if (!BaseType::isValid($basetype)) {
            throw new InvalidTypeException(sprintf('Base type "%s" is not valid.', $basetype));
        }

        switch ($basetype) {
            case BaseType::BOOLEAN:
                return self::TYPE_BOOL;
            case BaseType::DATE:
                return self::TYPE_DATE;
            case BaseType::FLOAT:
                return self::TYPE_FLOAT64;
            case BaseType::INTEGER:
                return self::TYPE_INT64;
            case BaseType::NUMERIC:
                return self::TYPE_NUMERIC;
            case BaseType::STRING:
                return self::TYPE_STRING;
            case BaseType::TIMESTAMP:
                return self::TYPE_TIMESTAMP;
        }

        throw new LogicException(sprintf('Definition for base type "%s" is missing.', $basetype));
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

    /**
     * @param null|int|string $length
     * @throws InvalidLengthException
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    private function validateLength(string $type, $length = null): void
    {
        $valid = true;
        switch (strtoupper($type)) {
            case self::TYPE_BYTES:
            case self::TYPE_STRING:
                $valid = $this->validateMaxLength($length, self::MAX_LENGTH);
                break;
            case self::TYPE_NUMERIC:
            case self::TYPE_DECIMAL:
                $valid = $this->validateBigqueryNumericLength($length, 38, 9);
                break;
            case self::TYPE_BIGNUMERIC:
            case self::TYPE_BIGDECIMAL:
                $valid = $this->validateBigNumericLength($length, 76, 38);
                break;
            case self::TYPE_ARRAY:
            case self::TYPE_STRUCT:
                break; // We don't check for this types
            default:
                if ($length !== null && $length !== '') {
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
     * @param null|int|string $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    protected function validateBigqueryNumericLength(
        $length,
        int $firstMax,
        int $secondMax
    ): bool {
        if ($this->isEmpty($length)) {
            return true;
        }

        $valid = $this->validateNumericLength($length, $firstMax, $secondMax);
        if (!$valid) {
            return false;
        }

        return $this->validateNumericScaleAndPrecision((string) $length, self::NUMERIC_LENGTH_CONST);
    }

    /**
     * @param null|int|string $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    protected function validateBigNumericLength(
        $length,
        int $firstMax,
        int $secondMax
    ): bool {
        if ($this->isEmpty($length)) {
            return true;
        }

        $valid = $this->validateNumericLength($length, $firstMax, $secondMax);
        if (!$valid) {
            return false;
        }

        return $this->validateNumericScaleAndPrecision((string) $length, self::BIGNUMERIC_LENGTH_CONST);
    }

    private function validateNumericScaleAndPrecision(string $length, int $decimalLengthConst): bool
    {
        $parts = explode(',', $length);
        $p = (int) $parts[0];
        $s = !isset($parts[1]) ? 0 : (int) $parts[1];
        // max(1, S) ≤ P ≤ S + <lengthConst NUMERIC=29|BIGNUMERIC=38>
        if ((max(1, $s) <= $p) && ($p <= ($s + $decimalLengthConst))) {
            return true;
        }

        return false;
    }
}
