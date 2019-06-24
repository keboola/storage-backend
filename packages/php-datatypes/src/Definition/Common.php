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
    protected $length = null;

    /**
     * @var bool
     */
    protected $nullable = true;

    /**
     * @var string
     */
    protected $default = null;

    /**
     * Common constructor.
     *
     * @param string $type
     * @param array $options -- length, nullable, default
     */
    public function __construct($type, $options = [])
    {
        $this->type = $type;
        if (isset($options['length'])) {
            $this->length = (string) $options['length'];
        }
        if (isset($options['nullable'])) {
            $this->nullable = (bool) $options['nullable'];
        }
        if (isset($options['default'])) {
            $this->default = (string) $options['default'];
        }
    }

    /**
     * @return string
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @return string|null
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
     * @return string|null
     */
    public function getDefault()
    {
        return $this->default;
    }

    /**
     * @return string
     */
    abstract public function getSQLDefinition();

    /**
     * @return string
     */
    abstract public function getBasetype();

    /**
     * @return array
     */
    abstract public function toArray();

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
        if (!is_null($this->getDefault())) {
            $metadata[] = [
                "key" => "KBC.datatype.default",
                "value" => $this->getDefault()
            ];
        }
        return $metadata;
    }
}
