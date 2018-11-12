<?php
namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
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
        "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
        "VARIANT",
        "BINARY","VARBINARY",
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
        $this->validateLength($type, isset($options["length"]) ? $options["length"] : null);
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
     * @param string $type
     * @throws InvalidTypeException
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
     */
    private function validateLength($type, $length = null)
    {
        $valid = true;
        switch (strtoupper($type)) {
            case "NUMBER":
            case "DECIMAL":
            case "NUMERIC":
                if (is_null($length) || $length === "") {
                    break;
                }
                $parts = explode(",", $length);
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
                if ((int)$parts[0] <= 0 || (int)$parts[0] > 38) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int)$parts[1] > (int)$parts[0] || (int)$parts[1] > 38)) {
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
            case "TIME":
            case "DATETIME":
            case "TIMESTAMP":
            case "TIMESTAMP_NTZ":
            case "TIMESTAMP_LTZ":
            case "TIMESTAMP_TZ":
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length < 0 || (int)$length > 9) {
                    $valid = false;
                    break;
                }
                break;
            case "BINARY":
            case "VARBINARY":
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length < 1 || (int)$length > 8388608) {
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
