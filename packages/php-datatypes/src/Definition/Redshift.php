<?php
namespace Keboola\Datatype\Definition;

use Keboola\Datatype\Definition\Exception\InvalidCompressionException;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

class Redshift extends Common
{
    const TYPES = [
        "SMALLINT", "INT2", "INTEGER", "INT", "INT4", "BIGINT", "INT8",
        "DECIMAL", "NUMERIC",
        "REAL", "FLOAT4", "DOUBLE PRECISION", "FLOAT8", "FLOAT",
        "BOOLEAN", "BOOL",
        "CHAR", "CHARACTER", "NCHAR", "BPCHAR",
        "VARCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT",
        "DATE",
        "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE",
        "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"
    ];

    protected $compression;

    /**
     * Redshift constructor.
     *
     * @param $type
     * @param null $length
     * @param bool $nullable
     * @param null $compression
     */
    public function __construct($type, $length = null, $nullable = false, $compression = null)
    {
        $this->validateType($type);
        $this->validateLength($type, $length);
        $this->validateCompression($type, $compression);
        parent::__construct($type, $length, $nullable);
        $this->compression = $compression;
    }

    /**
     * @return mixed
     */
    public function getCompression()
    {
        return $this->compression;
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
            case "CHARACTER VARYING":
            case "TEXT":
            case "NVARCHAR":
            case "TEXT":
                if (is_null($length) || $length == "") {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length <= 0 || (int)$length > 65535) {
                    $valid = false;
                    break;
                }
                break;
            case "CHAR":
            case "CHARACTER":
            case "NCHAR":
            case "BPCHAR":
                if (is_null($length)) {
                    break;
                }
                if (!is_numeric($length)) {
                    $valid = false;
                    break;
                }
                if ((int)$length <= 0 || (int)$length > 4096) {
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

    private function validateCompression($type, $compression)
    {
        $valid = true;
        $type = strtoupper($type);
        switch (strtoupper($compression)) {
            case 'RAW':
            case 'ZSTD':
            case 'RUNLENGTH':
            case null:
            case '':
                break;
            case 'BYTEDICT':
                if (in_array($type, ["BOOLEAN", "BOOL"])) {
                    $valid = false;
                }
                break;
            case 'DELTA':
                if (!in_array($type, ["SMALLINT", "INT2", "INT", "INTEGER", "INT4", "BIGINT", "INT8", "DATE", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMPTZ", "TIMESTAMP WITH TIMEZONE", "DECIMAL", "NUMERIC"])) {
                    $valid = false;
                }
                break;
            case 'DELTA32K':
                if (!in_array($type, ["INT", "INTEGER", "INT4", "BIGINT", "INT8", "DATE", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMPTZ", "TIMESTAMP WITH TIMEZONE", "DECIMAL", "NUMERIC"])) {
                    $valid = false;
                }
                break;
            case 'LZO':
                if (in_array($type, ["BOOLEAN", "BOOL", "REAL", "FLOAT4", "DOUBLE PRECISION", "FLOAT8", "FLOAT"])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY8':
                if (!in_array($type, ["SMALLINT", "INT2", "INT", "INTEGER", "INT4", "BIGINT", "INT8", "DECIMAL", "NUMERIC"])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY16':
                if (!in_array($type, ["INT", "INTEGER", "INT4", "BIGINT", "INT8", "DECIMAL", "NUMERIC"])) {
                    $valid = false;
                }
                break;
            case 'MOSTLY32':
                if (!in_array($type, ["BIGINT", "INT8", "DECIMAL", "NUMERIC"])) {
                    $valid = false;
                }
                break;
            case 'TEXT255':
            case 'TEXT32K':
                if (!in_array($type, ["VARCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT"])) {
                    $valid = false;
                }
                break;
            default:
                $valid = false;
                break;
        }
        if (!$valid) {
            throw new InvalidCompressionException("'{$compression}' is not valid compression for {$type}");
        }

    }
}
