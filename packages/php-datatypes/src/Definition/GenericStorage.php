<?php

namespace Keboola\Datatype\Definition;

class GenericStorage extends Common
{
    /**
     * @var string
     */
    protected $format = null;

    /**
     * Base constructor.
     * @param $type
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
        if ($this->default) {
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
        $metadata[] = [
            "key" => "KBC.datatype.format",
            "value" => $this->format
        ];
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
     * @return string
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
        if (stristr($this->type, "date")) {
            if (stristr($this->type, "time")) {
                return "TIMESTAMP";
            }
            return "DATE";
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
}
