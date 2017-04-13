<?php
namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

class Snowflake extends Common
{
    const TYPES = [
        "NUMBER",
        "DECIMAL", "NUMERIC",
        "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT",
        "FLOAT", "FLOAT4", "FLOAT8",
        "DOUBLE", "DOUBLE PRECISION", "REAL",
        "VARCHAR",
        "CHAR", "CHARACTER",
        "STRING", "TEXT",
        "BOOLEAN",
        "DATE",
        "DATETIME",
        "TIME",
        "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"
    ];

    /**
     * Snowflake constructor.
     *
     * @param $type
     * @param null $length
     * @param bool $nullable
     */
    public function __construct($type, $length = null, $nullable = false)
    {
        parent::__construct($type, $length, $nullable);
        $this->validateType($type);
        $this->validateLength($type, $length);
    }

    /**
     * @param $type
     * @throws InvalidTypeException
     */
    protected function validateType($type)
    {
        if (!in_array($type, $this::TYPES)) {
            throw new InvalidTypeException("{$type} is not a valid type");
        }
    }

    /**
     * @param $type
     * @param null $length
     * @throws InvalidLengthException
     */
    protected function validateLength($type, $length = null)
    {
        $valid = true;
        switch ($type) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                if (is_null($length)) {
                    break;
                }
                $parts = explode(",", $length);
                if (count($parts) != 2) {
                    $valid = false;
                    break;
                }
                if (!is_numeric($parts[0]) || !is_numeric($parts[1])) {
                    $valid = false;
                    break;
                }
                if ((int)$parts[0] <= 0 || (int)$parts[0] > 38 || (int)$parts[1] >= (int)$parts[0]) {
                    $valid = false;
                    break;
                }
                break;
            case "VARCHAR":
            case "CHAR":
            case "CHARACTER":
            case "STRING":
            case "TEXT":
                if (is_null($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length <= 0 || (int)$length > 16777216) {
                    $valid = false;
                    break;
                }
                break;
            default:
                if (!is_null($length)) {
                    $valid = false;
                    break;
                }
                break;
        }
        if (!$valid) {
            throw new InvalidLengthException("{$length} is not valid length for {$type}");
        }
    }
}
