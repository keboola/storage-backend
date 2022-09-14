<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use Exception;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use LogicException;

/**
 * Class Teradata
 *
 * https://docs.teradata.com/r/Ri8d7iL59tIPr1FZNKPLMw/TQAE5zgqV8pvyhrySc7ZVg
 */
class Teradata extends Common
{
    //https://docs.teradata.com/r/Ri8d7iL59tIPr1FZNKPLMw/DlfSbsVEC48atCIcADa5IA
    /* numbers */
    public const TYPE_BYTEINT = 'BYTEINT'; // -128 to 127, 1B, BYTEINT [ attributes [...] ]
    public const TYPE_BIGINT = 'BIGINT'; // 64bit signed, 7B, BIGINT [ attributes [...] ]
    public const TYPE_SMALLINT = 'SMALLINT'; //  -32768 to 32767, 2B, SMALLINT [ attributes [...] ]
    public const TYPE_INTEGER = 'INTEGER'; // 32bit signed, 4B, { INTEGER | INT } [ attributes [...] ]
    public const TYPE_INT = 'INT'; // = INTEGER
    public const TYPE_DECIMAL = 'DECIMAL'; // fixed length up to 16B
    // DECIMAL [(n[,m])], { DECIMAL | DEC | NUMERIC } [ ( n [, m ] ) ] [ attributes [...] ], 12.4567 : n = 6; m = 4.
    // n: 1-38 ; m 0-n, default when no n nor m -> DECIMAL(5, 0)., default when n is specified -> DECIMAL(n, 0).
    public const TYPE_NUMERIC = 'NUMERIC'; // = DECIMAL
    public const TYPE_DEC = 'DEC'; // = DECIMAL
    public const TYPE_FLOAT = 'FLOAT'; // 8B, { FLOAT | REAL | DOUBLE PRECISION } [ attributes [...] ]
    public const TYPE_DOUBLE_PRECISION = 'DOUBLE PRECISION'; // = FLOAT
    public const TYPE_REAL = 'REAL'; // = FLOAT
    public const TYPE_NUMBER = 'NUMBER'; // 1-20B,  NUMBER(n[,m]) / NUMBER[(*[,m])], as DECIMAL but variable-length
    // n: 1-38 ; m 0-n, default when no n nor m -> DECIMAL(5, 0)., default when n is specified DECIMAL(n, 0).

    /* Byte */
    public const TYPE_BYTE = 'BYTE'; // BYTE [ ( n ) ] [ attributes [...] ]; n Max 64000 Bytes; fixed length
    public const TYPE_VARBYTE = 'VARBYTE'; // VARBYTE ( n ) [ attributes [...] ]; n Max 64000 Bytes; VARIABLE length
    public const TYPE_BLOB = 'BLOB';
    //  { BINARY LARGE OBJECT | BLOB }
    //  [ ( n [ K | M | G ] ) ]
    //  [ attribute [...] ]
    // n - amount of
    //  Bytes - no unit
    //  K - K - max 2047937
    //  M - M - max 1999
    //  G - G - 1 only
    public const TYPE_BINARY_LARGE_OBJECT = 'BINARY LARGE OBJECT';
    /* DateTime */
    public const TYPE_DATE = 'DATE'; // DATE [ attributes [...] ]
    // TIME [ ( n ) ] [ attributes [...] ];
    // n = A single digit representing the number of digits in the fractional portion of the SECOND field.
    // '11:37:58.12345' n = 5; '11:37:58' n = 0
    public const TYPE_TIME = 'TIME';
    public const TYPE_TIMESTAMP = 'TIMESTAMP'; //  TIMESTAMP [ ( n ) ] [ attributes [...] ]
    // '1999-01-01 23:59:59.1234' n = 4
    // '1999-01-01 23:59:59' n = 0

    public const TYPE_TIME_WITH_ZONE = 'TIME_WITH_ZONE'; // as TIME but
    // TIME [ ( n ) ] WITH ZONE [ attributes [...] ]
    // where insert value as a number from -12.59 to +14.00  23:59:59.1234 +02:00
    public const TYPE_TIMESTAMP_WITH_ZONE = 'TIMESTAMP_WITH_ZONE'; // same as TYPE_TIME_WITH_ZONE
    /* character */
    public const TYPE_CHAR = 'CHAR'; // [(n)]
    public const TYPE_CHARACTER = 'CHARACTER'; // = CHAR
    //  { { CHARACTER | CHAR } [ ( n ) ]
    //  [ { CHARACTER | CHAR } SET server_character_set ] |
    //      GRAPHIC [ ( n ) ]
    //  } [ attributes [...] ]
    // n = length, static
    // 64000 for LATIN charset, 32000 for UNICODE,GRAPHIC,KANJISJIS

    public const TYPE_VARCHAR = 'VARCHAR';
    //  {
    //      { VARCHAR | { CHARACTER | CHAR } VARYING } ( n )
    //      [ { CHARACTER | CHAR } SET ] server_character_set |
    //      LONG VARCHAR |
    //      VARGRAPHIC ( n ) |
    //      LONG VARGRAPHIC
    //  } [ attributes [...] ]
    // n = length, variable
    // 64000 for LATIN charset, 32000 for UNICODE,GRAPHIC,KANJISJIS
    public const TYPE_CHARV = 'CHAR VARYING'; // = VARCHAR
    public const TYPE_CHARACTERV = 'CHARACTER VARYING'; // = VARCHAR
    public const TYPE_VARGRAPHIC = 'VARGRAPHIC'; // = VARCHAR
    // = VARCHAR but without n
    public const TYPE_LONG_VARCHAR = 'LONG VARCHAR'; // = VARCHAR with max n
    public const TYPE_LONG_VARGRAPHIC = 'LONG VARGRAPHIC'; // = LONG VARCHAR
    public const TYPE_CLOB = 'CLOB';
    // { CHARACTER LARGE OBJECT | CLOB }
    // [ ( n [ K | M | G ] ) ]
    // [ { CHARACTER | CHAR } SET { LATIN | UNICODE } ]
    // [ attribute [...] ]
    //  n - amount of
    //   Bytes - no unit
    //   K - K - max 2047937 for Latin, 1023968 for Unicode
    //   M - M - max 1999 for Latin, 999 for Unicode
    //   G - G - 1 and for LATIN only
    public const TYPE_CHARACTER_LARGE_OBJECT = 'CHARACTER LARGE OBJECT'; // = CLOB
    // Following types are listed due compatibility but they are treated as string
    /* Array */
    // not implemented, because arrays are considered as user defined types

    /* Period */
    // n represents fraction of seconds as in TIME / TIMESTAMP
    public const TYPE_PERIOD_DATE = 'PERIOD(DATE)'; // PERIOD(DATE)
    public const TYPE_PERIOD_TIME = 'PERIOD(TIME)';  // PERIOD(TIME [ ( n ) ] )
    public const TYPE_PERIOD_TIMESTAMP = 'PERIOD TIMESTAMP';  // PERIOD(TIMESTAMP [ ( n ) ] )
    public const TYPE_PERIOD_TIME_WITH_ZONE = 'PERIOD TIME WITH_ZONE';  // PERIOD(TIME [ ( n ) ] _WITH_ZONE )
    // PERIOD(TIMESTAMP [ ( n ) ] _WITH_ZONE )
    public const TYPE_PERIOD_TIMESTAMP_WITH_ZONE = 'PERIOD TIMESTAMP WITH_ZONE';
    /* Intervals */
    // n is always number of digits, m number of decimal digits for seconds.
    // INTERVAL HOUR(1) TO SECOND(2) = '9:59:59.99'
    public const TYPE_INTERVAL_SECOND = 'INTERVAL SECOND'; // INTERVAL SECOND [(n;[m])]
    public const TYPE_INTERVAL_MINUTE = 'INTERVAL MINUTE'; // INTERVAL MINUTE [(n)]
    public const TYPE_INTERVAL_MINUTE_TO_SECOND = 'INTERVAL MINUTE TO SECOND'; // INTERVAL MINUTE [(n)] TO SECOND [(m)]
    public const TYPE_INTERVAL_HOUR = 'INTERVAL HOUR'; // INTERVAL HOUR [(n)]
    public const TYPE_INTERVAL_HOUR_TO_SECOND = 'INTERVAL HOUR TO SECOND'; // INTERVAL HOUR [(n)] TO SECOND [(m)]
    public const TYPE_INTERVAL_HOUR_TO_MINUTE = 'INTERVAL HOUR TO MINUTE'; // INTERVAL HOUR [(n)] TO MINUTE
    public const TYPE_INTERVAL_DAY = 'INTERVAL DAY'; // INTERVAL DAY [(n)]
    public const TYPE_INTERVAL_DAY_TO_SECOND = 'INTERVAL DAY TO SECOND'; // INTERVAL DAY [(n)] TO SECOND [(m)]
    public const TYPE_INTERVAL_DAY_TO_MINUTE = 'INTERVAL DAY TO MINUTE'; // INTERVAL DAY [(n)] TO MINUTE
    public const TYPE_INTERVAL_DAY_TO_HOUR = 'INTERVAL DAY TO HOUR'; // INTERVAL DAY [(n)] TO HOUR
    public const TYPE_INTERVAL_MONTH = 'INTERVAL MONTH'; // INTERVAL MONTH
    public const TYPE_INTERVAL_YEAR = 'INTERVAL YEAR'; // INTERVAL YEAR [(n)]
    public const TYPE_INTERVAL_YEAR_TO_MONTH = 'INTERVAL YEAR TO MONTH'; // INTERVAL YEAR [(n)] TO MONTH
    // User Defined Types (UDP) are not supported

    // default lengths for different kinds of types. Used max values
    public const DEFAULT_BLOB_LENGTH = '1G';
    public const DEFAULT_BYTE_LENGTH = 64000;
    public const DEFAULT_DATETIME_DIGIT_LENGTH = 4;
    public const DEFAULT_DECIMAL_LENGTH = '38,19';
    public const DEFAULT_LATIN_CHAR_LENGTH = 64000;
    public const DEFAULT_LATIN_CLOB_LENGTH = '1999M';
    public const DEFAULT_NON_LATIN_CHAR_LENGTH = 32000;
    public const DEFAULT_NON_LATIN_CLOB_LENGTH = '999M';
    public const DEFAULT_SECOND_PRECISION_LENGTH = 6;
    public const DEFAULT_VALUE_TO_SECOND_PRECISION_LENGTH = '4,6';
    // types where length isnt at the end of the type
    public const COMPLEX_LENGTH_DICT = [
        self::TYPE_TIME_WITH_ZONE => 'TIME (%d) WITH TIME ZONE',
        self::TYPE_TIMESTAMP_WITH_ZONE => 'TIMESTAMP (%d) WITH TIME ZONE',
        self::TYPE_PERIOD_TIME => 'PERIOD(TIME (%d))',
        self::TYPE_PERIOD_TIMESTAMP => 'PERIOD(TIMESTAMP (%d))',
        self::TYPE_PERIOD_TIME_WITH_ZONE => 'PERIOD(TIME (%d) WITH TIME ZONE)',
        self::TYPE_PERIOD_TIMESTAMP_WITH_ZONE => 'PERIOD(TIMESTAMP (%d) WITH TIME ZONE)',
        self::TYPE_INTERVAL_DAY_TO_MINUTE => 'INTERVAL DAY (%d) TO MINUTE',
        self::TYPE_INTERVAL_DAY_TO_HOUR => 'INTERVAL DAY (%d) TO HOUR',
        self::TYPE_INTERVAL_HOUR_TO_MINUTE => 'INTERVAL HOUR (%d) TO MINUTE',
        self::TYPE_INTERVAL_MINUTE_TO_SECOND => 'INTERVAL MINUTE (%d) TO SECOND (%d)',
        self::TYPE_INTERVAL_HOUR_TO_SECOND => 'INTERVAL HOUR (%d) TO SECOND (%d)',
        self::TYPE_INTERVAL_DAY_TO_SECOND => 'INTERVAL DAY (%d) TO SECOND (%d)',
        self::TYPE_INTERVAL_YEAR_TO_MONTH => 'INTERVAL YEAR (%d) TO MONTH',

    ];
    /**
     * Types without precision, scale, or length
     * This used to separate types when column is retrieved from database
     */
    public const TYPES_WITHOUT_LENGTH = [
        self::TYPE_BYTEINT,
        self::TYPE_BIGINT,
        self::TYPE_SMALLINT,
        self::TYPE_INTEGER,
        self::TYPE_INT, //
        self::TYPE_FLOAT,
        self::TYPE_DOUBLE_PRECISION, //
        self::TYPE_REAL, //
        self::TYPE_PERIOD_DATE,
        self::TYPE_LONG_VARCHAR,
        self::TYPE_LONG_VARGRAPHIC,
    ];
    // syntax "TYPEXXX <length>" even if the length is not a single value, such as 38,38
    public const TYPES_WITH_SIMPLE_LENGTH = [
        self::TYPE_BYTE,
        self::TYPE_VARBYTE,
        self::TYPE_TIME,
        self::TYPE_TIMESTAMP,
        self::TYPE_CHAR,
        self::TYPE_CHARACTER, //
        self::TYPE_VARCHAR,
        self::TYPE_CHARV, //
        self::TYPE_CHARACTERV,
        self::TYPE_VARGRAPHIC,
        self::TYPE_INTERVAL_MINUTE,
        self::TYPE_INTERVAL_HOUR,
        self::TYPE_INTERVAL_DAY,
        self::TYPE_INTERVAL_MONTH,
        self::TYPE_INTERVAL_YEAR,
        self::TYPE_DECIMAL,
        self::TYPE_NUMERIC, // alias
        self::TYPE_DEC, //
        self::TYPE_NUMBER,
        self::TYPE_BLOB, // ?????
        self::TYPE_BINARY_LARGE_OBJECT, //
        self::TYPE_CLOB, // ???
        self::TYPE_CHARACTER_LARGE_OBJECT, //
        self::TYPE_INTERVAL_SECOND,

    ];
    // types where CHARAET can be defined
    public const CHARACTER_SET_TYPES = [
        self::TYPE_CHAR,
        self::TYPE_VARCHAR,
        self::TYPE_CLOB,

        self::TYPE_CHAR,
        self::TYPE_CHARV,
        self::TYPE_CHARACTERV,
        self::TYPE_VARGRAPHIC,
        self::TYPE_LONG_VARCHAR,
        self::TYPE_LONG_VARGRAPHIC,
        self::TYPE_CHARACTER_LARGE_OBJECT,
    ];
    //https://docs.teradata.com/r/rgAb27O_xRmMVc_aQq2VGw/6CYL2QcAvXykzEc8mG__Xg
    public const CODE_TO_TYPE = [
        'I8' => self::TYPE_BIGINT,
        'BO' => self::TYPE_BLOB,
        'BF' => self::TYPE_BYTE,
        'BV' => self::TYPE_VARBYTE,
        'I1' => self::TYPE_BYTEINT,
        'CF' => self::TYPE_CHAR,
        'CV' => self::TYPE_VARCHAR,
        'CO' => self::TYPE_CLOB,
        'D' => self::TYPE_DECIMAL,
        'DA' => self::TYPE_DATE,
        'F' => self::TYPE_FLOAT,
        'I' => self::TYPE_INTEGER,
        'DY' => self::TYPE_INTERVAL_DAY,
        'DH' => self::TYPE_INTERVAL_DAY_TO_HOUR,
        'DM' => self::TYPE_INTERVAL_DAY_TO_MINUTE,
        'DS' => self::TYPE_INTERVAL_DAY_TO_SECOND,
        'HR' => self::TYPE_INTERVAL_HOUR,
        'HM' => self::TYPE_INTERVAL_HOUR_TO_MINUTE,
        'HS' => self::TYPE_INTERVAL_HOUR_TO_SECOND,
        'MI' => self::TYPE_INTERVAL_MINUTE,
        'MS' => self::TYPE_INTERVAL_MINUTE_TO_SECOND,
        'MO' => self::TYPE_INTERVAL_MONTH,
        'SC' => self::TYPE_INTERVAL_SECOND,
        'YR' => self::TYPE_INTERVAL_YEAR,
        'YM' => self::TYPE_INTERVAL_YEAR_TO_MONTH,
        'N' => self::TYPE_NUMBER,
        'PD' => self::TYPE_PERIOD_DATE,
        'PT' => self::TYPE_PERIOD_TIME,
        'PZ' => self::TYPE_PERIOD_TIME_WITH_ZONE,
        'PS' => self::TYPE_PERIOD_TIMESTAMP,
        'PM' => self::TYPE_PERIOD_TIMESTAMP_WITH_ZONE,
        'I2' => self::TYPE_SMALLINT,
        'AT' => self::TYPE_TIME,
        'TS' => self::TYPE_TIMESTAMP,
        'TZ' => self::TYPE_TIME_WITH_ZONE,
        'SZ' => self::TYPE_TIMESTAMP_WITH_ZONE,
    ];
    public const TYPES = [
        self::TYPE_BIGINT,
        self::TYPE_BLOB,
        self::TYPE_BYTE,
        self::TYPE_VARBYTE,
        self::TYPE_BYTEINT,
        self::TYPE_CHARACTER,
        self::TYPE_VARCHAR,
        self::TYPE_CLOB,
        self::TYPE_DECIMAL,
        self::TYPE_DATE,
        self::TYPE_FLOAT,
        self::TYPE_INTEGER,
        self::TYPE_INTERVAL_DAY,
        self::TYPE_INTERVAL_DAY_TO_HOUR,
        self::TYPE_INTERVAL_DAY_TO_MINUTE,
        self::TYPE_INTERVAL_DAY_TO_SECOND,
        self::TYPE_INTERVAL_HOUR,
        self::TYPE_INTERVAL_HOUR_TO_MINUTE,
        self::TYPE_INTERVAL_HOUR_TO_SECOND,
        self::TYPE_INTERVAL_MINUTE,
        self::TYPE_INTERVAL_MINUTE_TO_SECOND,
        self::TYPE_INTERVAL_MONTH,
        self::TYPE_INTERVAL_SECOND,
        self::TYPE_INTERVAL_YEAR,
        self::TYPE_INTERVAL_YEAR_TO_MONTH,
        self::TYPE_NUMBER,
        self::TYPE_PERIOD_DATE,
        self::TYPE_PERIOD_TIME,
        self::TYPE_PERIOD_TIME_WITH_ZONE,
        self::TYPE_PERIOD_TIMESTAMP,
        self::TYPE_PERIOD_TIMESTAMP_WITH_ZONE,
        self::TYPE_SMALLINT,
        self::TYPE_TIME,
        self::TYPE_TIMESTAMP,
        self::TYPE_TIME_WITH_ZONE,
        self::TYPE_TIMESTAMP_WITH_ZONE,

        // aliases
        self::TYPE_INT,
        self::TYPE_CHAR,
        self::TYPE_CHARV,
        self::TYPE_CHARACTERV,
        self::TYPE_VARGRAPHIC,
        self::TYPE_LONG_VARCHAR,
        self::TYPE_LONG_VARGRAPHIC,
        self::TYPE_CHARACTER_LARGE_OBJECT,
        self::TYPE_BINARY_LARGE_OBJECT,
        self::TYPE_NUMERIC,
        self::TYPE_DEC,
        self::TYPE_FLOAT,
        self::TYPE_DOUBLE_PRECISION,
        self::TYPE_REAL,
    ];

    // these types expect invalid default length given TD. e.g. NUMBER returns -128,-128. -> has to be redefined
    public const TYPES_WITH_INVALID_DEFAULT_LENGTH = [
        self::TYPE_NUMBER => '-128,-128',
    ];

    private bool $isLatin = false;

    // depends on Char Type column in HELP TABLE column
    // 1 latin, 2 unicode, 3 kanjiSJIS, 4 graphic, 5 graphic, 0 others

    /**
     * @param array{length?:string|null, nullable?:bool, default?:string|null, isLatin?:bool} $options
     * @throws InvalidLengthException
     * @throws InvalidOptionException
     * @throws InvalidTypeException
     */
    public function __construct(string $type, array $options = [])
    {
        if (isset($options['isLatin'])) {
            $this->isLatin = (boolean) $options['isLatin'];
        }

        $this->validateType($type);
        $options['length'] = $this->consolidateLength($type, $options['length'] ?? null);
        $this->validateLength($type, $options['length'] ?? null);
        $diff = array_diff(array_keys($options), ['length', 'nullable', 'default', 'isLatin']);
        if ($diff !== []) {
            throw new InvalidOptionException("Option '{$diff[0]}' not supported");
        }
        parent::__construct($type, $options);
    }

    private function consolidateLength(string $type, ?string $length = null): ?string
    {
        if ($length !== null
            && array_key_exists($type, self::TYPES_WITH_INVALID_DEFAULT_LENGTH)
            && $length === self::TYPES_WITH_INVALID_DEFAULT_LENGTH[$type]
        ) {
            return null;
        }
        return $length;
    }

    /**
     * @throws \Exception
     */
    public static function convertCodeToType(string $code): string
    {
        if (!array_key_exists($code, self::CODE_TO_TYPE)) {
            throw new Exception("Type code {$code} is not supported");
        }

        return self::CODE_TO_TYPE[$code];
    }

    public function getSQLDefinition(): string
    {
        $definition = $this->buildTypeWithLength();

        if (!$this->isNullable()) {
            $definition .= ' NOT NULL';
        }
        if ($this->getDefault() !== null) {
            $definition .= ' DEFAULT ' . $this->getDefault();
        }

        if (in_array($this->getType(), self::CHARACTER_SET_TYPES, true)) {
            $definition .= ' CHARACTER SET ' . ($this->isLatin() ? 'LATIN' : 'UNICODE');
        }

        return $definition;
    }

    /**
     * generates type with length
     * most of the types just append it, but some of them are complex and some have no length...
     * used here and in i/e lib
     */
    public function buildTypeWithLength(): string
    {
        $type = $this->getType();
        $definition = $type;
        if (!in_array($definition, self::TYPES_WITHOUT_LENGTH)) {
            $length = $this->getLength();
            $length ??= $this->getDefaultLength();
            // length is set, use it
            if ($length !== null && $length !== '') {
                if (in_array($definition, self::TYPES_WITH_SIMPLE_LENGTH)) {
                    $definition .= sprintf(' (%s)', $length);
                } elseif (array_key_exists($definition, self::COMPLEX_LENGTH_DICT)) {
                    $definition = $this->buildComplexLength($type, $length);
                }
            }
        }

        return $definition;
    }

    /**
     * builds SQL definition for types which don't just append the length behind the type name
     *
     * @param string|int|null $lengthString
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    private function buildComplexLength(string $type, $lengthString): string
    {
        $parts = explode(',', (string) $lengthString);
        return sprintf(self::COMPLEX_LENGTH_DICT[$type], ...$parts);
    }

    private function isLatin(): bool
    {
        return $this->isLatin;
    }

    /**
     * Unlike RS or SNFLK which sets default values for types to max
     * Synapse sets default length to min, so when length is empty we need to set maximum values
     * to maintain same behavior as with RS and SNFLK
     *
     * @return int|string|null
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ReturnTypeHint.MissingNativeTypeHint
    private function getDefaultLength()
    {
        $out = null;
        switch ($this->type) {
            // decimals
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
            case self::TYPE_DEC:
                // number
            case self::TYPE_NUMBER:
                $out = self::DEFAULT_DECIMAL_LENGTH;
                break;

            case self::TYPE_BLOB:
            case self::TYPE_BINARY_LARGE_OBJECT:
                $out = self::DEFAULT_BLOB_LENGTH;
                break;

            case self::TYPE_CLOB:
            case self::TYPE_CHARACTER_LARGE_OBJECT:
                $out = $this->isLatin() ? self::DEFAULT_LATIN_CLOB_LENGTH : self::DEFAULT_NON_LATIN_CLOB_LENGTH;
                break;

            case self::TYPE_TIME_WITH_ZONE:
            case self::TYPE_TIMESTAMP_WITH_ZONE:
            case self::TYPE_TIMESTAMP:
            case self::TYPE_TIME:
            case self::TYPE_PERIOD_TIME:
            case self::TYPE_PERIOD_TIME_WITH_ZONE:
            case self::TYPE_PERIOD_TIMESTAMP:
            case self::TYPE_PERIOD_TIMESTAMP_WITH_ZONE:
                $out = self::DEFAULT_SECOND_PRECISION_LENGTH;
                break;

            case self::TYPE_INTERVAL_DAY_TO_SECOND:
            case self::TYPE_INTERVAL_MINUTE_TO_SECOND:
            case self::TYPE_INTERVAL_HOUR_TO_SECOND:
            case self::TYPE_INTERVAL_SECOND:
                $out = self::DEFAULT_VALUE_TO_SECOND_PRECISION_LENGTH;
                break;

            case self::TYPE_INTERVAL_DAY_TO_MINUTE:
            case self::TYPE_INTERVAL_DAY_TO_HOUR:
            case self::TYPE_INTERVAL_HOUR_TO_MINUTE:
            case self::TYPE_INTERVAL_YEAR_TO_MONTH:
            case self::TYPE_INTERVAL_MINUTE:
            case self::TYPE_INTERVAL_HOUR:
            case self::TYPE_INTERVAL_DAY:
            case self::TYPE_INTERVAL_MONTH:
            case self::TYPE_INTERVAL_YEAR:
                $out = self::DEFAULT_DATETIME_DIGIT_LENGTH;
                break;

            case self::TYPE_BYTE:
            case self::TYPE_VARBYTE:
                $out = self::DEFAULT_BYTE_LENGTH;
                break;

            case self::TYPE_CHAR:
            case self::TYPE_CHARACTER:
            case self::TYPE_VARCHAR:
            case self::TYPE_CHARV:
            case self::TYPE_CHARACTERV:
            case self::TYPE_VARGRAPHIC:
                $out = $this->isLatin() ? self::DEFAULT_LATIN_CHAR_LENGTH : self::DEFAULT_NON_LATIN_CHAR_LENGTH;
                break;
        }

        return $out;
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
            case self::TYPE_DECIMAL:
            case self::TYPE_NUMERIC:
            case self::TYPE_DEC:
            case self::TYPE_NUMBER:
                $valid = $this->validateNumericLength($length, 38, 38);
                break;
            case self::TYPE_INTERVAL_SECOND:
            case self::TYPE_INTERVAL_MINUTE_TO_SECOND:
            case self::TYPE_INTERVAL_HOUR_TO_SECOND:
            case self::TYPE_INTERVAL_DAY_TO_SECOND:
                $valid = $this->validateNumericLength($length, 4, 6, false);
                break;
            case self::TYPE_TIME:
            case self::TYPE_TIMESTAMP:
            case self::TYPE_TIME_WITH_ZONE:
            case self::TYPE_TIMESTAMP_WITH_ZONE:
            case self::TYPE_PERIOD_TIME:
            case self::TYPE_PERIOD_TIME_WITH_ZONE:
            case self::TYPE_PERIOD_TIMESTAMP:
            case self::TYPE_PERIOD_TIMESTAMP_WITH_ZONE:
                $valid = $this->validateMaxLength($length, 6, 0);
                break;
            case self::TYPE_INTERVAL_MINUTE:
            case self::TYPE_INTERVAL_HOUR:
            case self::TYPE_INTERVAL_DAY:
            case self::TYPE_INTERVAL_MONTH:
            case self::TYPE_INTERVAL_YEAR:
            case self::TYPE_INTERVAL_DAY_TO_MINUTE:
            case self::TYPE_INTERVAL_HOUR_TO_MINUTE:
            case self::TYPE_INTERVAL_DAY_TO_HOUR:
            case self::TYPE_INTERVAL_YEAR_TO_MONTH:
                $valid = $this->validateMaxLength($length, 4);
                break;
            case self::TYPE_BYTE:
            case self::TYPE_VARBYTE:
                $valid = $this->validateMaxLength($length, 64000);
                break;

            case self::TYPE_CHAR:
            case self::TYPE_CHARACTER:
            case self::TYPE_VARCHAR:
            case self::TYPE_CHARV:
            case self::TYPE_CHARACTERV:
            case self::TYPE_VARGRAPHIC:
                $valid = $this->validateMaxLength($length, $this->isLatin() ? 64000 : 32000);
                break;
            case self::TYPE_CLOB:
            case self::TYPE_CHARACTER_LARGE_OBJECT:
                $isLatin = $this->isLatin();
                $valid = $this->validateLOBLength(
                    $length,
                    [
                        'noUnit' => $isLatin ? 2_097_088_000 : 1_048_544_000,
                        'K' => $isLatin ? 2_047_937 : 1_023_968,
                        'M' => $isLatin ? 1999 : 999,
                        'G' => $isLatin ? 1 : 0,
                    ]
                );
                break;
            case self::TYPE_BLOB:
            case self::TYPE_BINARY_LARGE_OBJECT:
                $valid = $this->validateLOBLength(
                    $length,
                    [
                        'noUnit' => 2_097_088_000,
                        'K' => 2_047_937,
                        'M' => 1999,
                        'G' => 1,
                    ]
                );
                break;
        }

        if (!$valid) {
            throw new InvalidLengthException("'{$length}' is not valid length for {$type}");
        }
    }

    /**
     * @param array<string, int> $maxTab table (array) with max values
     * @param null|int|string $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    private function validateLOBLength($length, array $maxTab): bool
    {
        if ($this->isEmpty($length)) {
            return true;
        }
        if (!preg_match('/^([1-9]\d*)\s*(M|K|G)?$/', (string) $length, $out)) {
            return false;
        }
        if (count($out) === 2) {
            // no unit
            return $out[1] < $maxTab['noUnit'] && $out[1] >= 1;
        }
        if (count($out) === 3) {
            // with unit
            return $out[1] <= $maxTab[$out[2]] && $out[1] >= 1;
        }
        return false;
    }

    public function getBasetype(): string
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
            case self::TYPE_DATE:
                $basetype = BaseType::DATE;
                break;
            case self::TYPE_TIME:
            case self::TYPE_TIME_WITH_ZONE:
            case self::TYPE_TIMESTAMP:
            case self::TYPE_TIMESTAMP_WITH_ZONE:
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
