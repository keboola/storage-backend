<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

/**
 * Class Synapse
 *
 * DOCS for types:
 * https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#DataTypes
 * https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-tables-data-types
 */
class Synapse extends Common
{
    public const METADATA_BACKEND = 'synapse';
    public const TYPE_DECIMAL = 'DECIMAL';
    public const TYPE_NUMERIC = 'NUMERIC';
    public const TYPE_FLOAT = 'FLOAT';
    public const TYPE_REAL = 'REAL';
    public const TYPE_MONEY = 'MONEY';
    public const TYPE_SMALLMONEY = 'SMALLMONEY';
    public const TYPE_BIGINT = 'BIGINT';
    public const TYPE_INT = 'INT';
    public const TYPE_SMALLINT = 'SMALLINT';
    public const TYPE_TINYINT = 'TINYINT';
    public const TYPE_BIT = 'BIT';
    public const TYPE_NVARCHAR = 'NVARCHAR';
    public const TYPE_NCHAR = 'NCHAR';
    public const TYPE_VARCHAR = 'VARCHAR';
    public const TYPE_CHAR = 'CHAR';
    public const TYPE_VARBINARY = 'VARBINARY';
    public const TYPE_BINARY = 'BINARY';
    public const TYPE_UNIQUEIDENTIFIER = 'UNIQUEIDENTIFIER';
    public const TYPE_DATETIMEOFFSET = 'DATETIMEOFFSET';
    public const TYPE_DATETIME2 = 'DATETIME2';
    public const TYPE_DATETIME = 'DATETIME';
    public const TYPE_SMALLDATETIME = 'SMALLDATETIME';
    public const TYPE_DATE = 'DATE';
    public const TYPE_TIME = 'TIME';

    public const TYPES = [
        self::TYPE_DECIMAL, self::TYPE_NUMERIC,
        self::TYPE_FLOAT, self::TYPE_REAL,
        self::TYPE_MONEY, self::TYPE_SMALLMONEY,
        self::TYPE_BIGINT, self::TYPE_INT, self::TYPE_SMALLINT, self::TYPE_TINYINT,
        self::TYPE_BIT,
        self::TYPE_NVARCHAR,self::TYPE_NCHAR,self::TYPE_VARCHAR,self::TYPE_CHAR,
        self::TYPE_VARBINARY,self::TYPE_BINARY,
        self::TYPE_UNIQUEIDENTIFIER,
        self::TYPE_DATETIMEOFFSET,self::TYPE_DATETIME2,self::TYPE_DATETIME,self::TYPE_SMALLDATETIME,self::TYPE_DATE,
        self::TYPE_TIME,
    ];

    public const MAX_LENGTH_NVARCHAR = 4000;
    public const MAX_LENGTH_BINARY = 8000;
    public const MAX_LENGTH_FLOAT = 53;
    public const MAX_LENGTH_NUMERIC = '38,0';

    /**
     * Types with precision and scale
     * This used to separate (precision,scale) types from length types when column is retrieved from database
     */
    public const TYPES_WITH_COMPLEX_LENGTH = [
        self::TYPE_DECIMAL, self::TYPE_NUMERIC,
    ];

    /**
     * Types without precision, scale, or length
     * This used to separate types when column is retrieved from database
     */
    public const TYPES_WITHOUT_LENGTH = [
        Synapse::TYPE_DATETIME,
        Synapse::TYPE_REAL,
        Synapse::TYPE_SMALLDATETIME,
        Synapse::TYPE_DATE,
        Synapse::TYPE_MONEY,
        Synapse::TYPE_SMALLMONEY,
        Synapse::TYPE_BIGINT,
        Synapse::TYPE_INT,
        Synapse::TYPE_SMALLINT,
        Synapse::TYPE_TINYINT,
        Synapse::TYPE_BIT,
        Synapse::TYPE_UNIQUEIDENTIFIER,
    ];

    /**
     * @param array{length?:string|null, nullable?:bool, default?:string|null} $options
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
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
        $length = $this->getLength();
        if ($length !== null && $length !== '') {
            $definition .= sprintf('(%s)', $length);
        } else {
            $length = $this->getDefaultLength();
            if ($length !== null) {
                $definition .= sprintf('(%s)', $length);
            }
        }
        if (!$this->isNullable()) {
            $definition .= ' NOT NULL';
        }
        if ($this->getDefault() !== null) {
            $definition .= ' DEFAULT ' . $this->getDefault();
        }
        return $definition;
    }

    /**
     * Unlike RS or SNFLK which sets default values for types to max
     * Synapse sets default length to min, so when length is empty we need to set maximum values
     * to maintain same behavior as with RS and SNFLK
     *
     * @return int|string|null
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ReturnTypeHint.MissingNativeTypeHint
    public function getDefaultLength()
    {
        switch (strtoupper($this->getType())) {
            case self::TYPE_FLOAT:
                return self::MAX_LENGTH_FLOAT;
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                return self::MAX_LENGTH_NUMERIC;
            case self::TYPE_NCHAR:
            case self::TYPE_NVARCHAR:
                return self::MAX_LENGTH_NVARCHAR;
            case self::TYPE_BINARY:
            case self::TYPE_CHAR:
            case self::TYPE_VARBINARY:
            case self::TYPE_VARCHAR:
                return self::MAX_LENGTH_BINARY;
        }

        return null;
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
        if (!in_array(strtoupper($type), $this::TYPES, true)) {
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
            case self::TYPE_FLOAT:
                $valid = $this->validateMaxLength($length, 53);
                break;
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                $valid = $this->validateNumericLength($length, 38, 38);
                break;
            case self::TYPE_NVARCHAR:
                if ($this->isEmpty($length)) {
                    break;
                }
                if (strtoupper((string) $length) === 'MAX') {
                    break;
                }
                $valid = $this->validateMaxLength($length, 4000);
                break;
            case self::TYPE_NCHAR:
                $valid = $this->validateMaxLength($length, 4000);
                break;
            case self::TYPE_VARBINARY:
            case self::TYPE_VARCHAR:
                if ($this->isEmpty($length)) {
                    break;
                }
                if (strtoupper((string) $length) === 'MAX') {
                    break;
                }
                $valid = $this->validateMaxLength($length, 8000);
                break;
            case self::TYPE_BINARY:
            case self::TYPE_CHAR:
                $valid = $this->validateMaxLength($length, 8000);
                break;
            case self::TYPE_DATETIMEOFFSET:
            case self::TYPE_DATETIME2:
            case self::TYPE_TIME:
                $valid = $this->validateMaxLength($length, 7, 0);
                break;
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

    public function getBasetype(): string
    {
        switch (strtoupper($this->type)) {
            case self::TYPE_INT:
            case self::TYPE_BIGINT:
            case self::TYPE_SMALLINT:
            case self::TYPE_TINYINT:
                $basetype = BaseType::INTEGER;
                break;
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                $basetype = BaseType::NUMERIC;
                break;
            case self::TYPE_FLOAT:
            case self::TYPE_REAL:
                $basetype = BaseType::FLOAT;
                break;
            case self::TYPE_BIT:
                $basetype = BaseType::BOOLEAN;
                break;
            case self::TYPE_DATE:
                $basetype = BaseType::DATE;
                break;
            case self::TYPE_DATETIMEOFFSET:
            case self::TYPE_DATETIME:
            case self::TYPE_DATETIME2:
            case self::TYPE_SMALLDATETIME:
            case self::TYPE_TIME:
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
                return self::TYPE_BIT;
            case BaseType::DATE:
                return self::TYPE_DATE;
            case BaseType::FLOAT:
                return self::TYPE_FLOAT;
            case BaseType::INTEGER:
                return self::TYPE_INT;
            case BaseType::NUMERIC:
                return self::TYPE_NUMERIC;
            case BaseType::STRING:
                return self::TYPE_NVARCHAR;
            case BaseType::TIMESTAMP:
                return self::TYPE_DATETIME2;
        }

        throw new LogicException(sprintf('Definition for base type "%s" is missing.', $basetype));
    }
}
