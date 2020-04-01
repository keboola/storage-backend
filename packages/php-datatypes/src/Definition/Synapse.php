<?php
namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

/**
 * Class Synapse
 *
 * DOCS for types:
 * https://docs.microsoft.com/en-us/sql/t-sql/statements/create-table-azure-sql-data-warehouse?view=aps-pdw-2016-au7#DataTypes
 * https://docs.microsoft.com/en-us/azure/sql-data-warehouse/sql-data-warehouse-tables-data-types
 */
class Synapse extends Common
{
    const TYPE_DECIMAL = 'decimal';
    const TYPE_NUMERIC = 'numeric';
    const TYPE_FLOAT = 'float';
    const TYPE_REAL = 'real';
    const TYPE_MONEY = 'money';
    const TYPE_SMALLMONEY = 'smallmoney';
    const TYPE_BIGINT = 'bigint';
    const TYPE_INT = 'int';
    const TYPE_SMALLINT = 'smallint';
    const TYPE_TINYINT = 'tinyint';
    const TYPE_BIT = 'bit';
    const TYPE_NVARCHAR = 'nvarchar';
    const TYPE_NCHAR = 'nchar';
    const TYPE_VARCHAR = 'varchar';
    const TYPE_CHAR = 'char';
    const TYPE_VARBINARY = 'varbinary';
    const TYPE_BINARY = 'binary';
    const TYPE_UNIQUEIDENTIFIER = 'uniqueidentifier';
    const TYPE_DATETIMEOFFSET = 'datetimeoffset';
    const TYPE_DATETIME2 = 'datetime2';
    const TYPE_DATETIME = 'datetime';
    const TYPE_SMALLDATETIME = 'smalldatetime';
    const TYPE_DATE = 'date';
    const TYPE_TIME = 'time';

    const TYPES = [
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

    const MAX_LENGTH_NVARCHAR = 4000;
    const MAX_LENGTH_BINARY = 8000;
    const MAX_LENGTH_FLOAT = 53;
    const MAX_LENGTH_NUMERIC = '38,0';

    /**
     * Types with precision and scale
     * This used to separate (precision,scale) types from length types when column is retrieved from database
     */
    const TYPES_WITH_COMPLEX_LENGTH = [
        self::TYPE_DECIMAL, self::TYPE_NUMERIC,
    ];

    /**
     * Types without precision, scale, or length
     * This used to separate types when column is retrieved from database
     */
    const TYPES_WITHOUT_LENGTH = [
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
     * @param string $type
     * @param array $options -- length, nullable, default
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function __construct($type, $options = [])
    {
        $this->validateType($type);
        $this->validateLength($type, isset($options['length']) ? $options['length'] : null);
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default']);
        if (count($diff) > 0) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }
        parent::__construct($type, $options);
    }

    /**
     * @return string
     */
    public function getSQLDefinition()
    {
        $definition = $this->getType();
        $length = $this->getLength();
        if ($length !== null && $length !== '') {
            $definition .= sprintf('(%s)', $length);
        } else {
            $length = $this->getDefaultLength();
            if (null !== $length) {
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
    private function getDefaultLength()
    {
        switch (strtolower($this->getType())) {
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
     * @param string|null $length
     * @return bool
     */
    private function isEmpty($length)
    {
        return $length === null || $length === '';
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return [
            'type' => $this->getType(),
            'length' => $this->getLength(),
            'nullable' => $this->isNullable(),
        ];
    }

    /**
     * @param string $type
     * @return void
     * @throws InvalidTypeException
     */
    private function validateType($type)
    {
        if (!in_array(strtolower($type), $this::TYPES, true)) {
            throw new InvalidTypeException(sprintf('"%s" is not a valid type', $type));
        }
    }

    /**
     * @param string $type
     * @param string|null $length
     * @return void
     * @throws InvalidLengthException
     */
    private function validateLength($type, $length = null)
    {
        $valid = true;
        switch (strtolower($type)) {
            case self::TYPE_FLOAT:
                if ($this->isEmpty($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ($length < 1 || $length > 53) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                if ($length === null || $length === '') {
                    break;
                }
                $parts = explode(',', $length);
                $countParts = count($parts);
                if ($countParts > 2 || $countParts < 1) {
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
                if (isset($parts[1]) && ((int) $parts[1] > (int) $parts[0] || (int) $parts[1] > 38)) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_NVARCHAR:
                if ($this->isEmpty($length)) {
                    break;
                }
                if ($length === 'max') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 1 || (int) $length > 4000) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_NCHAR:
                if ($this->isEmpty($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 1 || (int) $length > 4000) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_VARBINARY:
            case self::TYPE_VARCHAR:
                if ($this->isEmpty($length)) {
                    break;
                }
                if ($length === 'max') {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 1 || (int) $length > 8000) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_BINARY:
            case self::TYPE_CHAR:
                if ($this->isEmpty($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 1 || (int) $length > 8000) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_DATETIMEOFFSET:
            case self::TYPE_DATETIME2:
            case self::TYPE_TIME:
                if ($this->isEmpty($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 0 || (int) $length > 7) {
                    $valid = false;
                    break;
                }
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

    /**
     * @return string
     */
    public function getBasetype()
    {
        switch (strtolower($this->type)) {
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
}
