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
        if (stristr($this->type, "date")) {
            if (stristr($this->type, "time")) {
                return "TIMESTAMP";
            }
            return "DATE";
        } else if (stristr($this->type, "int")) {
            return "INTEGER";
        } else if (stripos($this->type, "float") === 0 || stripos($this->type, "real") === 0) {
            return "FLOAT";
        } else if (stristr($this->type, "timestamp")) {
            return "TIMESTAMP";
        } else if (stripos($this->type, "bool") === 0) {
            return "BOOLEAN";
        } else if (stripos($this->type, "decimal") === 0 ||
            stripos($this->type, "num") === 0 ||
            stristr($this->type, "double")) {
            return "NUMERIC";
        }
        return "STRING";
    }
}
