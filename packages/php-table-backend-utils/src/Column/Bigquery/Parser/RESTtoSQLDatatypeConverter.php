<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser;

use Exception;
use Keboola\Datatype\Definition\Bigquery;

/**
 * @phpstan-import-type BigqueryTableFieldSchema from Bigquery
 */
final class RESTtoSQLDatatypeConverter
{

    /**
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     */
    public static function convertColumnToSQLFormat(array $dbResponse): Bigquery
    {
        $type = self::getType($dbResponse['type']);
        if (self::isRepeated($dbResponse)) {
            $type = Bigquery::TYPE_ARRAY;
        }
        $default = $dbResponse['defaultValueExpression'] ?? null;
        $length = self::getTypeLength($type, $dbResponse);

        /** @var array{length?:string|null, nullable?:bool, default?:string|null} $options */
        $options = [
            'nullable' => self::isNullable($dbResponse),
            'length' => $length,
            'default' => $default,
        ];
        return new Bigquery(
            $type,
            $options,
        );
    }

    /**
     * Convert types returned by api to valid types for BQ
     * tables can't be created with this types
     */
    private static function getType(string $type): string
    {
        return match ($type) {
            'FLOAT' => Bigquery::TYPE_FLOAT64,
            'BOOLEAN' => Bigquery::TYPE_BOOL,
            'RECORD' => Bigquery::TYPE_STRUCT,
            default => $type
        };
    }

    /**
     * Returns type length as string for Bigquery datatype from php-datatypes
     *
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     */
    private static function getTypeLength(string $type, array $dbResponse): ?string
    {
        return match ($type) {
            Bigquery::TYPE_BYTES, Bigquery::TYPE_STRING => $dbResponse['maxLength'] ?? null,
            Bigquery::TYPE_NUMERIC, Bigquery::TYPE_BIGNUMERIC => self::getLengthForNumber($dbResponse),
            Bigquery::TYPE_ARRAY, Bigquery::TYPE_STRUCT => self::getRecordTypeLength($dbResponse, true),
            default => null
        };
    }

    /**
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     */
    private static function isNullable(array $dbResponse): bool
    {
        return array_key_exists('mode', $dbResponse) ? $dbResponse['mode'] !== 'REQUIRED' : true;
    }

    /**
     * Returns precision,scale in format used in php-datatypes as string
     *
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     */
    private static function getLengthForNumber(array $dbResponse): ?string
    {
        if (array_key_exists('precision', $dbResponse) === false) {
            // when precision is not set, set length to null
            return null;
        }
        $precision = $dbResponse['precision'];

        if (array_key_exists('scale', $dbResponse) === false) {
            return (string) $precision;
        }

        $scale = $dbResponse['scale'];

        return $precision . ',' . $scale;
    }

    /**
     * Converts length of complex types to string used in php-datatypes
     *
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     * @param bool $ignoreTopLevelType - do not print top level name and type as only length of this type should be
     *     printed
     */
    private static function getRecordTypeLength(array $dbResponse, bool $ignoreTopLevelType = false): string
    {
        $isStruct = self::isStruct($dbResponse['type']);
        $isRepeated = self::isRepeated($dbResponse);

        if ($isRepeated && !$isStruct) {
            // array is not struct is not recursive and contain only type
            $length = self::getTypeLength($dbResponse['type'], $dbResponse);

            if ($length !== null) {
                // wrap length in brackets used in SQL
                $length = '(' . $length . ')';
            }
            if ($ignoreTopLevelType) {
                // do not wrap first level definition in ARRAY<>
                return self::getType($dbResponse['type']) . $length;
            }
            // wrap nested type in ARRAY<> as it is length string definition used in BQ
            return $dbResponse['name'] . ' ARRAY<' . self::getType($dbResponse['type']) . $length . '>';
        }

        if ($isRepeated) {
            // if struct is repeated it needs to be wrapped in ARRAY<>
            $wrap = 'ARRAY<STRUCT<%s>>';
        } elseif ($isStruct) {
            $wrap = 'STRUCT<%s>';
        } else {
            throw new Exception('Only struct or repeated type can be handled.');
        }

        $content = [];
        // we can assert existence of fields as they are always part of structs
        assert(array_key_exists('fields', $dbResponse), 'Fields must be set.');
        /** @phpstan-var BigqueryTableFieldSchema $field */
        foreach ($dbResponse['fields'] as $field) {
            if (self::isStruct($field['type']) || self::isRepeated($field)) {
                // do recursive call if type is complex
                $content[] = self::getRecordTypeLength($field);
                continue;
            }
            $length = self::getTypeLength($field['type'], $field);

            if ($length !== null) {
                // wrap length in brackets used in SQL
                $length = '(' . $length . ')';
            }

            $content[] = $field['name'] . ' ' . self::getType($field['type']) . $length;
        }

        $content = implode(',', $content);

        if ($ignoreTopLevelType) {
            // wrap is ignored for top level type as it is length string definition used in BQ
            if ($isRepeated) {
                return sprintf('STRUCT<%s>', $content);
            }
            return $content;
        }

        return $dbResponse['name'] . ' ' . sprintf($wrap, $content);
    }

    /**
     * @phpstan-param BigqueryTableFieldSchema $dbResponse
     */
    private static function isRepeated(array $dbResponse): bool
    {
        return array_key_exists('mode', $dbResponse) && $dbResponse['mode'] === 'REPEATED';
    }

    private static function isStruct(string $type): bool
    {
        return $type === 'RECORD';
    }
}
