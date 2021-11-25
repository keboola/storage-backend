<?php
namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

class Snowflake extends Common
{
    const TYPE_NUMBER = 'NUMBER';
    const TYPE_DECIMAL = 'DECIMAL';
    const TYPE_NUMERIC = 'NUMERIC';
    const TYPE_INT = 'INT';
    const TYPE_INTEGER = 'INTEGER';
    const TYPE_BIGINT = 'BIGINT';
    const TYPE_SMALLINT = 'SMALLINT';
    const TYPE_TINYINT = 'TINYINT';
    const TYPE_BYTEINT = 'BYTEINT';
    const TYPE_FLOAT = 'FLOAT';
    const TYPE_FLOAT4 = 'FLOAT4';
    const TYPE_FLOAT8 = 'FLOAT8';
    const TYPE_DOUBLE = 'DOUBLE';
    const TYPE_DOUBLE_PRECISION = 'DOUBLE PRECISION';
    const TYPE_REAL = 'REAL';
    const TYPE_VARCHAR = 'VARCHAR';
    const TYPE_CHAR = 'CHAR';
    const TYPE_CHARACTER = 'CHARACTER';
    const TYPE_STRING = 'STRING';
    const TYPE_TEXT = 'TEXT';
    const TYPE_BOOLEAN = 'BOOLEAN';
    const TYPE_DATE = 'DATE';
    const TYPE_DATETIME = 'DATETIME';
    const TYPE_TIME = 'TIME';
    const TYPE_TIMESTAMP = 'TIMESTAMP';
    const TYPE_TIMESTAMP_NTZ = 'TIMESTAMP_NTZ';
    const TYPE_TIMESTAMP_LTZ = 'TIMESTAMP_LTZ';
    const TYPE_TIMESTAMP_TZ = 'TIMESTAMP_TZ';
    const TYPE_VARIANT = 'VARIANT';
    const TYPE_BINARY = 'BINARY';
    const TYPE_VARBINARY = 'VARBINARY';
    const TYPES = [
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

    /**
     * Snowflake constructor.
     *
     * @param string $type
     * @param array $options -- length, nullable, default
     * @throws InvalidOptionException
     */
    public function __construct($type, $options = [])
    {
        $this->validateType($type);
        $options['length'] = $this->processLength($options);
        $this->validateLength($type, $options['length']);
        $diff = array_diff(array_keys($options), ["length", "nullable", "default"]);
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
        $definition =  $this->getType();
        if ($this->getLength() !== null && $this->getLength() !== "") {
            $definition .= "(" . $this->getLength() . ")";
        }
        if (!$this->isNullable()) {
            $definition .= " NOT NULL";
        }
        return $definition;
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return [
            "type" => $this->getType(),
            "length" => $this->getLength(),
            "nullable" => $this->isNullable()
        ];
    }

    /**
     * @param array $options
     * @return string|null
     * @throws InvalidOptionException
     */
    private function processLength($options)
    {
        if (!isset($options['length'])) {
            return null;
        }
        if (is_array($options['length'])) {
            return $this->getLengthFromArray($options['length']);
        }
        return $options['length'];
    }

    /**
     * @param array $lengthOptions
     * @throws InvalidOptionException
     * @return null|string
     */
    private function getLengthFromArray($lengthOptions)
    {
        $expectedOptions = ['character_maximum', 'numeric_precision', 'numeric_scale'];
        $diff = array_diff(array_keys($lengthOptions), $expectedOptions);
        if (count($diff) > 0) {
            throw new InvalidOptionException(sprintf('Length option "%s" not supported', $diff[0]));
        }

        $characterMaximum = isset($lengthOptions['character_maximum']) ? $lengthOptions['character_maximum'] : null;
        $numericPrecision = isset($lengthOptions['numeric_precision']) ? $lengthOptions['numeric_precision'] : null;
        $numericScale = isset($lengthOptions['numeric_scale']) ? $lengthOptions['numeric_scale'] : null;

        if (!is_null($characterMaximum)) {
            return $characterMaximum;
        }
        if (!is_null($numericPrecision) && !is_null($numericScale)) {
            return $numericPrecision . ',' . $numericScale;
        }
        return $numericPrecision;
    }

    /**
     * @param string $type
     * @throws InvalidTypeException
     * @return void
     */
    private function validateType($type)
    {
        if (!in_array(strtoupper($type), $this::TYPES)) {
            throw new InvalidTypeException("'{$type}' is not a valid type");
        }
    }

    /**
     * @param string $type
     * @param string|null $length
     * @throws InvalidLengthException
     * @return void
     */
    private function validateLength($type, $length = null)
    {
        $valid = true;
        switch (strtoupper($type)) {
            case self::TYPE_NUMBER:
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
                if (is_null($length) || $length === "") {
                    break;
                }
                $parts = explode(",", $length);
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
                if (isset($parts[1]) && ((int) $parts[1] > (int) $parts[0] || (int) $parts[1] > 38)) {
                    $valid = false;
                    break;
                }
                break;
            case self::TYPE_VARCHAR:
            case self::TYPE_CHAR:
            case self::TYPE_CHARACTER:
            case self::TYPE_STRING:
            case self::TYPE_TEXT:
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length <= 0 || (int) $length > 16777216) {
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
                if (is_null($length) || $length == "") {
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
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int) $length < 1 || (int) $length > 8388608) {
                    $valid = false;
                    break;
                }
                break;
            default:
                if (!is_null($length) && $length != "") {
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
}
