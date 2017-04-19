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
     * @param array $options -- length, nullable, default
     */
    public function __construct($type, $options = [])
    {
        $this->validateType($type);
        if (isset($options['length'])) {
            $this->validateLength($type, $options['length']);
        }
        parent::__construct($type, $options);
    }

    /**
     * @param $type
     * @throws InvalidTypeException
     */
    private function validateType($type)
    {
        if (!in_array(strtoupper($type), $this::TYPES)) {
            throw new InvalidTypeException("'{$type}' is not a valid type");
        }
    }

    /**
     * @param $type
     * @param null $length
     * @throws InvalidLengthException
     */
    private function validateLength($type, $length = null)
    {
        $valid = true;
        switch (strtoupper($type)) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                if (is_null($length) || $length == "") {
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
                if (is_null($length) || $length == "") {
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

    public function getBasetype()
    {
        switch (strtoupper($this->type)) {
            case "INT":
            case "INTEGER":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
            case "BYTEINT":
                $basetype = "INTEGER";
                break;
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                $basetype = "NUMERIC";
                break;
            case "FLOAT":
            case "FLOAT4":
            case "FLOAT8":
            case "DOUBLE":
            case "DOUBLE PRECISION":
            case "REAL":
                $basetype = "FLOAT";
                break;
            case "BOOLEAN":
                $basetype = "BOOLEAN";
                break;
            case "DATE":
                $basetype = "DATE";
                break;
            case "DATETIME":
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
                $basetype = "TIMESTAMP";
                break;
            default:
                $basetype = "STRING";
                break;
        }
        return $basetype;
    }
}
