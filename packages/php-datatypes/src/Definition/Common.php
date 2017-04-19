<?php

namespace Keboola\Datatype\Definition;

abstract class Common
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
    protected $nullable;
    /**
     * @var string
     */
    protected $default;

    /**
     * Common constructor.
     *
     * @param $type
     * @param array $options --  length, nullable, default
     */
    public function __construct($type, $options = [])
    {
        $this->type = $type;
        $this->length = (isset($options['length'])) ? $options['length'] : "";
        $this->nullable = (isset($options['nullable'])) ? (bool) $options['nullable'] : true;
        if (isset($options['default'])) {
            $this->default = $options['default'];
        }
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
    abstract public function getBasetype();

    /**
     * @return array
     */
    public function toArray()
    {
        $array = [
            "type" => $this->getType(),
            "length" => $this->getLength(),
            "nullable" => $this->isNullable(),
        ];
        if (!is_null($this->getDefault())) {
            $array["default"] = $this->getDefault();
        }
        return $array;
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
                "value" => $this->isNullable()
            ],[
                "key" => "KBC.datatype.basetype",
                "value" => $this->getBasetype()
            ]
        ];
        if ($this->getLength()) {
            $metadata[] = [
                "key" => "KBC.datatype.length",
                "value" => $this->getLength()
            ];
        }
        if ($this->getDefault()) {
            $metadata[] = [
                "key" => "KBC.datatype.default",
                "value" => $this->getDefault()
            ];
        }
        return $metadata;
    }
}
