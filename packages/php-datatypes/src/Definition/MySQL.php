<?php
namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidOptionException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

class MySQL extends Common
{
    const TYPES = [
        "INTEGER", "INT",
        "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
        "DECIMAL", "DEC", "FIXED", "NUMERIC",
        "FLOAT", "DOUBLE PRECISION", "REAL", "DOUBLE",
        "BIT",
        "DATE",
        "TIME",
        "DATETIME",
        "TIMESTAMP",
        "YEAR",
        "CHAR",
        "VARCHAR",
        "BLOB",
        "TEXT"
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
        if ($this->getLength() && $this->getLength() != "") {
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
            case "CHAR":
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length <= 0 || (int)$length > 255) {
                    $valid = false;
                    break;
                }
                break;

            case "VARCHAR":
                if (is_null($length) || $length == "" || !is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length <= 0 || (int)$length > 4294967295) {
                    $valid = false;
                    break;
                }
                break;

            case "INT":
            case "INTEGER":
            case "TINYINT":
            case "SMALLINT":
            case "MEDIUMINT":
            case "BIGINT":
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length <= 0 || (int)$length > 255) {
                    $valid = false;
                    break;
                }
                break;

            case "DOUBLE PRECISION":
            case "REAL":
            case "DOUBLE":
            case "FLOAT":
                if (is_null($length) || $length == "") {
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
                if ((int)$parts[0] <= 0 || (int)$parts[0] > 255) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int)$parts[1] >= (int)$parts[0] || (int)$parts[1] > 30)) {
                    $valid = false;
                    break;
                }
                break;

            case "DECIMAL":
            case "DEC":
            case "FIXED":
            case "NUMERIC":
                if (is_null($length) || $length == "") {
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
                if ((int)$parts[0] <= 0 || (int)$parts[0] > 65) {
                    $valid = false;
                    break;
                }
                if (isset($parts[1]) && ((int)$parts[1] > (int)$parts[0] || (int)$parts[1] > 30)) {
                    $valid = false;
                    break;
                }
                break;

            case "TIME":
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length < 0 || (int)$length > 6) {
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
            case "MEDIUMINT":
                $basetype = BaseType::INTEGER;
                break;
            case "NUMERIC":
            case "DECIMAL":
            case "DEC":
            case "FIXED":
                $basetype = BaseType::NUMERIC;
                break;
            case "FLOAT":
            case "DOUBLE PRECISION":
            case "REAL":
            case "DOUBLE":
                $basetype = BaseType::FLOAT;
                break;
            case "DATE":
                $basetype = BaseType::DATE;
                break;
            case "DATETIME":
            case "TIMESTAMP":
                $basetype = BaseType::TIMESTAMP;
                break;
            default:
                $basetype = BaseType::STRING;
                break;
        }
        return $basetype;
    }
}
