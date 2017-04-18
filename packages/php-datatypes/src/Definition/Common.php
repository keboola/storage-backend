<?php

namespace Keboola\Datatype\Definition;

class Common
{
    /**
     * @var string
     */
    protected $type;
    /**
     * @var string
     */
    protected $length;
    /**
     * @var bool
     */
    protected $nullable = false;
    /**
     * @var string
     */
    protected $default;
    /**
     * @var string
     */
    protected $format;

    /**
     * Common constructor.
     *
     * @param $type
     * @param string $length
     * @param bool $nullable
     */

    public function __construct($type, $length = null, $nullable = true, $default = null, $format = null)
    {
        $this->type = $type;
        $this->length = $length;
        $this->nullable = (bool) $nullable;
        if ($nullable and is_null($default)) {
            $this->default = "NULL";
        } else {
            $this->default = $default;
        }
        $this->format = $format;
    }

    /**
     * @return mixed
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @return mixed
     */
    public function getLength()
    {
        return $this->length;
    }

    /**
     * @return boolean
     */
    public function isNullable()
    {
        return $this->nullable;
    }

    /**
     * @return string
     */
    public function getDefault()
    {
        return $this->default;
    }

    /**
     * @return string
     */
    public function getFormat()
    {
        return $this->format;
    }

    /**
     * @return string
     */
    public function getSQLDefinition()
    {
        if ($this->getLength() && $this->getLength() != "") {
            return $this->getType() . "(" . $this->getLength() . ")";
        }
        return $this->getType();
    }

    /**
     * @return string
     */
    public function getBaseType()
    {
        if (stristr($this->type,"date")) {
            if (stristr($this->type, "time")) {
                return 'TIMESTAMP';
            } else {
                return 'DATE';
            }
        }
        if (stristr($this->type, "int")) {
            return "INTEGER";
        }
        if (stristr($this->type, "float") || stristr($this->type, "double") || stristr($this->type, "real")) {
            return "FLOAT";
        }
        if (stristr($this->type, "timestamp")) {
            return "TIMESTAMP";
        }
        if (stristr($this->type, "bool")) {
            return "BOOLEAN";
        }
        if (stristr($this->type, "decimal") || stristr($this->type, "num")) {
            return "NUMERIC";
        }
        return "STRING";
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
     * @return array
     */
    public function toMetadata()
    {
        $metadata = [
            [
                "key" => "KBC.datatype.type",
                "value" => $this->getType(),
            ],[
                "key" => "KBC.datatype.nullable",
                "value" => $this->nullable
            ],[
                "key" => "KBC.datatype.basetype",
                "value" => $this->getBasetype()
            ]
        ];
        if ($this->length) {
            $metadata[] = [
                "key" => "KBC.datatype.length",
                "value" => $this->length
            ];
        }
        if ($this->default) {
            $metadata[] = [
                "key" => "KBC.datatype.default",
                "value" => $this->default
            ];
        }
        if ($this->format) {
            $metadata[] = [
                "key" => "KBC.datatype.format",
                "value" => $this->format
            ];
        }

        return $metadata;
    }
}
