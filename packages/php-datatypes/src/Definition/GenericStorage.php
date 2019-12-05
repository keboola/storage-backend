<?php

namespace Keboola\Datatype\Definition;

class GenericStorage extends Common
{
    const DATE_TYPES = ["date"];
    const TIMESTAMP_TYPES = [
        "datetime", "datetime2", "smalldatetime", "datetimeoffset", "timestamp_LTZ", "timestamp_NTZ", "TIMESTAMP_TZ",
        "timestamptz", "timestamp", "timestamp with time zone", "timestamp with local time zone", "timestamp without time zone"
    ];
    const FLOATING_POINT_TYPES = [
        "real", "float", "float4", "double precision", "float8", "binary_float", "binary_double", "double",
        "d_float", "quad"
    ];
    // NOTE: "bit" is used in mssql as a 1/0 type boolean, but in pgsql as a bit(n) ie 10110.
    // also in mysql bit is equivalent to tinyint
    const BOOLEAN_TYPES = ["boolean", "bool"];

    const INTEGER_TYPES = [
        "integer", "int", "smallint", "mediumint",
        "int2", "tinyint", "bigint", "int8", "bigserial", "serial8", "int4", "int64"
    ];

    const FIXED_NUMERIC_TYPES = [
        "numeric", "decimal", "dec", "fixed", "money", "smallmoney", "number"
    ];

    /**
     * @var string
     */
    protected $format = null;

    /**
     * Base constructor.
     * @param string $type
     * @param array $options
     */
    public function __construct($type, array $options = [])
    {
        parent::__construct($type, $options);
        if (isset($options['format'])) {
            $this->format = $options['format'];
        }
    }

    /**
     * @return string
     */
    public function getSQLDefinition()
    {
        $sql = $this->getType();
        if ($this->getLength() && $this->getLength() != "") {
            $sql .= "(" . $this->getLength() . ")";
        }
        $sql .= ($this->isNullable()) ? " NULL" : " NOT NULL";
        if (!is_null($this->getDefault())) {
            $sql .= " DEFAULT '" . $this->default . "'";
        } else if ($this->isNullable()) {
            $sql .= " DEFAULT NULL";
        }
        return $sql;
    }

    /**
     * @return array
     */
    public function toMetadata()
    {
        $metadata = parent::toMetadata();
        if (!is_null($this->getFormat())) {
            $metadata[] = [
                "key" => "KBC.datatype.format",
                "value" => $this->format
            ];
        }
        return $metadata;
    }

    /**
     * @return array
     */
    public function toArray()
    {
        $result = [
            "type" => $this->getType(),
            "length" => $this->getLength(),
            "nullable" => $this->isNullable()
        ];
        if (!is_null($this->getDefault())) {
            $result["default"] = $this->getDefault();
        }
        if ($this->getFormat()) {
            $result["format"] = $this->getFormat();
        }
        return $result;
    }

    /**
     * @return string|null
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @return string
     */
    public function getBasetype()
    {
        $type = strtolower($this->type);
        $baseType = BaseType::STRING;
        if (in_array($type, self::DATE_TYPES)) {
            $baseType = BaseType::DATE;
        }
        if (in_array($type, self::TIMESTAMP_TYPES)) {
            $baseType = BaseType::TIMESTAMP;
        }
        if (in_array($type, self::INTEGER_TYPES)) {
            $baseType = BaseType::INTEGER;
        }
        if (in_array($type, self::FIXED_NUMERIC_TYPES)) {
            $baseType = BaseType::NUMERIC;
        }
        if (in_array($type, self::FLOATING_POINT_TYPES)) {
            $baseType = BaseType::FLOAT;
        }
        if (in_array($type, self::BOOLEAN_TYPES)) {
            $baseType = BaseType::BOOLEAN;
        }
        return $baseType;
    }
}
