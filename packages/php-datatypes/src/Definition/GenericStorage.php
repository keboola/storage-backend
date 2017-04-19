<?php

namespace Keboola\Datatype\Definition;

class GenericStorage extends Common
{
    /**
     * @var string
     */
    protected $format;

    /**
     * Base constructor.
     * @param $type
     * @param array $options
     */
    public function __construct($type, array $options = [])
    {
        parent::__construct($type, $options);
        $this->format = (isset($options['format'])) ? $options['format'] : null;
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
        $sql .= ($this->nullable) ? " NULL" : " NOT NULL";
        if ($this->default) {
            $sql .= ($this->default === "NULL") ? " DEFAULT NULL" : " DEFAULT '" . $this->default . "'";
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
        $result = parent::toArray();
        if ($this->format) {
            $result['format'] = $this->format;
        }
        return $result;
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
}
