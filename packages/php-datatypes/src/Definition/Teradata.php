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
    //https://docs.teradata.com/r/Ri8d7iL59tIPr1FZNKPLMw/DlfSbsVEC48atCIcADa5IA
    /* numbers */
    const TYPE_BYTEINT = 'BYTEINT'; // -128 to 127, 1B, BYTEINT [ attributes [...] ]
    const TYPE_BIGINT = 'BIGINT'; // 64bit signed, 7B, BIGINT [ attributes [...] ]
    const TYPE_SMALLINT = 'SMALLINT'; //  -32768 to 32767, 2B, SMALLINT [ attributes [...] ]
    const TYPE_INTEGER = 'INTEGER'; // 32bit signed, 4B, { INTEGER | INT } [ attributes [...] ]
    const TYPE_INT = 'INT'; // = INTEGER

    const TYPE_DECIMAL = 'DECIMAL'; // fixed length up to 16B
    // DECIMAL [(n[,m])], { DECIMAL | DEC | NUMERIC } [ ( n [, m ] ) ] [ attributes [...] ], 12.4567 : n = 6; m = 4
    const TYPE_NUMERIC = 'NUMERIC'; // = DECIMAL
    const TYPE_DEC = 'DEC'; // = DECIMAL

    const TYPE_FLOAT = 'FLOAT'; // 8B, { FLOAT | REAL | DOUBLE PRECISION } [ attributes [...] ]
    const TYPE_DOUBLE_PRECISION = 'DOUBLE PRECISION'; // = FLOAT
    const TYPE_REAL = 'REAL'; // = FLOAT

    const TYPE_NUMBER = 'NUMBER'; // 1-20B,  NUMBER(n[,m]) / NUMBER[(*[,m])], as DECIMAL but variable-length


    /* Byte */
    const TYPE_BYTE = 'BYTE'; // BYTE [ ( n ) ] [ attributes [...] ]; n Max 64000 Bytes; fixed length
    const TYPE_VARBYTE = 'VARBYTE'; // VARBYTE ( n ) [ attributes [...] ]; n Max 64000 Bytes; VARIABLE length
    const TYPE_BLOB = 'BLOB';
    //  { BINARY LARGE OBJECT | BLOB }
    //  [ ( n [ K | M | G ] ) ]
    //  [ attribute [...] ]
    // n - amount of
    //  Bytes - no unit
    //  KB - K - max 2047937
    //  MB - M - max 1999
    //  GB - G - 1 only

    /* DateTime */
    const TYPE_DATE = 'DATE'; // DATE [ attributes [...] ]
    const TYPE_TIME = 'TIME'; // TIME [ ( n ) ] [ attributes [...] ]; n = A single digit representing the number of digits in the fractional portion of the SECOND field. '11:37:58.12345' n = 5; '11:37:58' n = 0
    const TYPE_TIMESTAMP = 'TIMESTAMP'; //  TIMESTAMP [ ( n ) ] [ attributes [...] ]
    // '1999-01-01 23:59:59.1234' n = 4
    // '1999-01-01 23:59:59' n = 0

    const TYPE_TIME_WITH_ZONE = 'TIME WITH TIME ZONE'; // as TIME but
    // TIME [ ( n ) ] WITH TIME ZONE [ attributes [...] ]
    // where insert value as a number from -12.59 to +14.00  23:59:59.1234 +02:00
    const TYPE_TIMESTAMP_WITH_ZONE = 'TIMESTAMP WITH TIME ZONE'; // same as TYPE_TIME_WITH_ZONE


    /* character */
    const TYPE_CHAR = 'CHAR'; // [(n)]
    const TYPE_CHARACTER = 'CHARACTER'; // = CHAR
    //  { { CHARACTER | CHAR } [ ( n ) ]
    //  [ { CHARACTER | CHAR } SET server_character_set ] |
    //      GRAPHIC [ ( n ) ]
    //  } [ attributes [...] ]
    // n = length, static
    // 64000 for LATIN charset, 32000 for UNICODE,GRAPHIC,KANJISJIS

    const TYPE_VARCHAR = 'VARCHAR';
    //  {
    //      { VARCHAR | { CHARACTER | CHAR } VARYING } ( n )
    //      [ { CHARACTER | CHAR } SET ] server_character_set |
    //      LONG VARCHAR |
    //      VARGRAPHIC ( n ) |
    //      LONG VARGRAPHIC
    //  } [ attributes [...] ]
    // n = length, variable
    // 64000 for LATIN charset, 32000 for UNICODE,GRAPHIC,KANJISJIS
    const TYPE_CHARV = 'CHAR VARYING'; // = VARCHAR
    const TYPE_CHARACTERV = 'CHARACTER VARYING'; // = VARCHAR
    const TYPE_VARGRAPHIC = 'VARGRAPHIC'; // = VARCHAR

    // = VARCHAR but without n
    const TYPE_LONG_VARCHAR = 'LONG VARCHAR';
    const TYPE_LONG_VARGRAPHIC = 'LONG VARGRAPHIC';

    const TYPE_CLOB = 'CLOB';
    // { CHARACTER LARGE OBJECT | CLOB }
    // [ ( n [ K | M | G ] ) ]
    // [ { CHARACTER | CHAR } SET { LATIN | UNICODE } ]
    // [ attribute [...] ]
    //  n - amount of
    //   Bytes - no unit
    //   KB - K - max 2047937 for Latin, 1023968 for Unicode
    //   MB - M - max 1999 for Latin, 999 for Unicode
    //   GB - G - 1 and for LATIN only
    const TYPE_CHARACTER_LARGE_OBJECT = 'CHARACTER LARGE OBJECT'; // = CLOB


    /* Array */
//    TODO
    const TYPE_ARRAY = 'ARRAY';
    const TYPE_VARRAY = 'VARRAY';

    /* Period */
//    TODO
    const TYPE_PERIOD = 'PERIOD'; // (DATE) / PERIOD(TIME [(n)]) / PERIOD(TIMESTAMP [(n)])

    /* Intervals */
//    TODO


// TODO User-defined Type

    /**
     * Types with precision and scale
     * This used to separate (precision,scale) types from length types when column is retrieved from database
     */
    const TYPES_WITH_COMPLEX_LENGTH = [
        self::TYPE_DECIMAL,
        self::TYPE_NUMERIC,
        self::TYPE_DEC,
        self::TYPE_NUMBER,

        self::TYPE_CLOB,
        self::TYPE_CHARACTER_LARGE_OBJECT,
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
//        self::TYPE_TIME_WITH_ZONE,
        self::TYPE_TIMESTAMP,
//        self::TYPE_TIMESTAMP_WITH_ZONE,
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
        switch (strtoupper($this->type)) {
            case self::TYPE_BYTEINT:
            case self::TYPE_INTEGER:
            case self::TYPE_INT:
            case self::TYPE_BIGINT:
            case self::TYPE_SMALLINT:
                $basetype = BaseType::INTEGER;
                break;
            case self::TYPE_DECIMAL:
            case self::TYPE_DEC:
            case self::TYPE_NUMERIC:
            case self::TYPE_NUMBER:
                $basetype = BaseType::NUMERIC;
                break;
            case self::TYPE_FLOAT:
            case self::TYPE_DOUBLE_PRECISION:
            case self::TYPE_REAL:
                $basetype = BaseType::FLOAT;
                break;
//            case self::TYPE_BIT:
//                $basetype = BaseType::BOOLEAN;
//                break;
            case self::TYPE_DATE:
                $basetype = BaseType::DATE;
                break;
            case self::TYPE_TIME:
            case self::TYPE_TIME_WITH_ZONE:
                $basetype = BaseType::TIMESTAMP;
                break;
            default:
                $basetype = BaseType::STRING;
                break;
        }
        return $basetype;
    }
}
