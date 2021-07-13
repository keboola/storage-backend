<?php

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

/**
 * Class Teradata
 *
 * https://docs.teradata.com/r/Ri8d7iL59tIPr1FZNKPLMw/TQAE5zgqV8pvyhrySc7ZVg
 */
class Teradata extends Common
{
    const TYPE_BLOB = 'BLOB';
    const TYPE_BYTE = 'BYTE';
    const TYPE_VARBYTE = 'VARBYTE';
    const TYPE_BIGINT = 'BIGINT';
    const TYPE_BYTEINT = 'BYTEINT';
    const TYPE_DATE = 'DATE';
    const TYPE_DECIMAL = 'DECIMAL';
    const TYPE_DOUBLE_PRECISION = 'DOUBLE PRECISION';
    const TYPE_FLOAT = 'FLOAT';
    const TYPE_INTEGER = 'INTEGER';
    const TYPE_NUMBER = 'NUMBER';
    const TYPE_NUMERIC = 'NUMERIC';
    const TYPE_REAL = 'REAL';
    const TYPE_SMALLINT = 'SMALLINT';
    const TYPE_TIME = 'TIME';
    const TYPE_TIME_WITH_ZONE = 'TIME WITH TIME ZONE';
    const TYPE_TIMESTAMP = 'TIMESTAMP';
    const TYPE_TIMESTAMP_WITH_ZONE = 'TIMESTAMP WITH TIME ZONE';
    const TYPE_CHAR = 'CHAR';
    const TYPE_LONG_VARCHAR = 'LONG VARCHAR';
    const TYPE_CLOB = 'CLOB';
    const TYPE_VARCHAR = 'VARCHAR';
    const TYPE_PERIOD = 'PERIOD';
// TODO intervals
// TODO User-defined Type
// TODO arrays
// TODO complex periods
// TODO varying 
// TODO long varchar

    /**
     * Types with precision and scale
     * This used to separate (precision,scale) types from length types when column is retrieved from database
     */
    const TYPES_WITH_COMPLEX_LENGTH = [
        self::TYPE_DECIMAL,
        self::TYPE_NUMERIC,
    ];
    /**
     * Types without precision, scale, or length
     * This used to separate types when column is retrieved from database
     */
    const TYPES_WITHOUT_LENGTH = [
        self::TYPE_BIGINT,
        self::TYPE_BYTEINT,
        self::TYPE_DATE,
        self::TYPE_DOUBLE_PRECISION,
        self::TYPE_FLOAT,
        self::TYPE_INTEGER,
        self::TYPE_REAL,
        self::TYPE_SMALLINT,
        self::TYPE_CLOB,
        self::TYPE_LONG_VARCHAR,
    ];
//https://docs.teradata.com/r/rgAb27O_xRmMVc_aQq2VGw/6CYL2QcAvXykzEc8mG__Xg
    const TYPE_CODE_TO_TYPE = [
        'I8' => self::TYPE_BIGINT,
        'BO' => self::TYPE_BLOB,
        'BF' => self::TYPE_BYTE,
        'I1' => self::TYPE_BYTEINT,
        'CF' => self::TYPE_CHAR,
        'CO' => self::TYPE_CLOB,
        'D' => self::TYPE_DECIMAL,
        // could be also NUMERIC 'D' => 'NUMERIC,
        'DA' => self::TYPE_DATE,
        'F' => self::TYPE_FLOAT,
        // 'DOUBLE PRECISION DOUBLE PRECISION, FLOAT, and REAL are different names for the same data type.,
        'I' => self::TYPE_INTEGER,
        'N' => self::TYPE_NUMBER,
        'PD' => self::TYPE_PERIOD,
        'I2' => self::TYPE_SMALLINT,
        'AT' => self::TYPE_TIME,
        'TS' => self::TYPE_TIMESTAMP,
        'TZ' => self::TYPE_TIME_WITH_ZONE,
        'SZ' => self::TYPE_TIMESTAMP_WITH_ZONE,
//        '++' => self::TYPE_TD_ANYTYPE,
//        'UT' => self::TYPE_USER‑DEFINED TYPE ,
//        'XM' => self::TYPE_XML,
    ];
    const TYPES = [
        self::TYPE_BLOB,
        self::TYPE_BYTE,
        self::TYPE_VARBYTE,
        self::TYPE_BIGINT,
        self::TYPE_BYTEINT,
        self::TYPE_DATE,
        self::TYPE_DECIMAL,
        self::TYPE_DOUBLE_PRECISION,
        self::TYPE_FLOAT,
        self::TYPE_INTEGER,
        self::TYPE_NUMBER,
        self::TYPE_NUMERIC,
        self::TYPE_REAL,
        self::TYPE_SMALLINT,
        self::TYPE_TIME,
        self::TYPE_TIME_WITH_ZONE,
        self::TYPE_TIMESTAMP,
        self::TYPE_TIMESTAMP_WITH_ZONE,
        self::TYPE_CHAR,
        self::TYPE_LONG_VARCHAR,
        self::TYPE_CLOB,
        self::TYPE_VARCHAR,
        self::TYPE_PERIOD,
    ];
// TODO intervals
// TODO User-defined Type
// TODO arrays
// TODO complex periods
// TODO varying (byte, character)
// TODO xml, json...
// TODO anytype

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

    public static function convertCodeToType($code)
    {
        if (!array_key_exists($code, self::TYPE_CODE_TO_TYPE)) {
            throw new \Exception("Type code {$code} is not supported");
        }

        return self::TYPE_CODE_TO_TYPE[$code];
    }


    /**
     * @return string
     */
    public function getSQLDefinition()
    {
        $definition = $this->getType();
        if (!in_array($definition, self::TYPES_WITHOUT_LENGTH)) {
            $length = $this->getLength();
            if ($length !== null && $length !== '') {
                $definition .= sprintf('(%s)', $length);
            } else {
                $length = $this->getDefaultLength();
                if (null !== $length) {
                    $definition .= sprintf('(%s)', $length);
                }
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
//        TODO

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
        if (!in_array(strtoupper($type), $this::TYPES, true)) {
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
//        TODO
    }

    /**
     * @return string
     */
    public function getBasetype()
    {
//        TODO

    }
}
