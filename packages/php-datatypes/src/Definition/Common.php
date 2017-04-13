<?php

namespace Keboola\Datatype\Definition;

use Keboola\Datatype\DatatypeInterface;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;

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

    const TYPES = [];

    /**
     * Common constructor.
     *
     * @param $type
     * @param string $length
     * @param bool $nullable
     */
    public function __construct($type, $length = null, $nullable = false)
    {
        $this->validateType($type);
        $this->type = $type;
        $this->validateLength($type, $length);
        $this->length = $length;
        $this->nullable = (bool) $nullable;
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
    public function getlength()
    {
        return $this->length;
    }

    /**
     * @param mixed $length
     * @return $this
     */
    public function setlength($length)
    {
        $this->length = $length;

        return $this;
    }

    /**
     * @return boolean
     */
    public function isNullable()
    {
        return $this->nullable;
    }

    /**
     * @param $type
     * @throws InvalidTypeException
     */
    protected function validateType($type)
    {
        if (!in_array($type, $this::TYPES)) {
            throw new InvalidTypeException("{$type} is not a valid type");
        }
    }

    /**
     * @param $type
     * @param $length
     */
    protected function validateLength($type, $length = null)
    {
    }
}
