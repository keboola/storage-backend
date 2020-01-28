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
    const TYPES = [
        'decimal', 'numeric',
        'float', 'real',
        'money', 'smallmoney',
        'bigint', 'int', 'smallint', 'tinyint',
        'bit',
        'nvarchar','nchar','varchar','char',
        'varbinary','binary',
        'uniqueidentifier',
        'datetimeoffset','datetime2','datetime','smalldatetime','date',
        'time',
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
        if ($length !== null && $length !== "") {
            $definition .= sprintf('(%s)', $length);
        }
        if (!$this->isNullable()) {
            $definition .= ' NOT NULL';
        }
        return $definition;
    }

    /**
     * @param $length
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
     * @throws InvalidLengthException
     */
    private function validateLength($type, $length = null)
    {
        $valid = true;
        switch (strtolower($type)) {
            case 'float':
            case 'real':
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
            case 'decimal':
            case 'numeric':
                if ($this->isEmpty($length)) {
                    break;
                }
                $parts = explode(',', $length);
                if (count($parts) > 2 || count($parts) < 1) {
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
            case 'nvarchar':
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
            case 'nchar':
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
            case 'varbinary':
            case 'varchar':
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
            case 'binary':
            case 'char':
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
            case 'datetimeoffset':
            case 'datetime2':
            case 'date':
            case 'time':
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
            case 'int':
            case 'bigint':
            case 'smallint':
            case 'tinyint':
                $basetype = BaseType::INTEGER;
                break;
            case 'decimal':
            case 'numeric':
                $basetype = BaseType::NUMERIC;
                break;
            case 'float':
            case 'real':
                $basetype = BaseType::FLOAT;
                break;
            case 'bit':
                $basetype = BaseType::BOOLEAN;
                break;
            case 'date':
                $basetype = BaseType::DATE;
                break;
            case 'datetimeoffset':
            case 'datetime':
            case 'datetime2':
            case 'smalldatetime':
            case 'time':
                $basetype = BaseType::TIMESTAMP;
                break;
            default:
                $basetype = BaseType::STRING;
                break;
        }
        return $basetype;
    }
}
