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
        $this->default = $default;
        if ($nullable and is_null($default)) {
            $this->default = "NULL";
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
    public function getBasetype()
    {
        $basetype = "STRING";
        if (stristr($this->type, "date")) {
            $basetype = 'DATE';
            if (stristr($this->type, "time")) {
                $basetype = 'TIMESTAMP';
            }
        }
        if (stristr($this->type, "int")) {
            $basetype = "INTEGER";
        }
        if (stristr($this->type, "float") || stristr($this->type, "double") || stristr($this->type, "real")) {
            $basetype = "FLOAT";
        }
        if (stristr($this->type, "timestamp")) {
            $basetype = "TIMESTAMP";
        }
        if (stristr($this->type, "bool")) {
            $basetype = "BOOLEAN";
        }
        if (stristr($this->type, "decimal") || stristr($this->type, "num")) {
            $basetype = "NUMERIC";
        }
        return $basetype;
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
