<?php

namespace Keboola\Datatype\Definition;

abstract class Common implements DefinitionInterface
{
    const KBC_METADATA_KEY_TYPE = "KBC.datatype.type";
    const KBC_METADATA_KEY_NULLABLE = "KBC.datatype.nullable";
    const KBC_METADATA_KEY_BASETYPE = "KBC.datatype.basetype";
    const KBC_METADATA_KEY_LENGTH = "KBC.datatype.length";
    const KBC_METADATA_KEY_DEFAULT = "KBC.datatype.default";

    const KBC_METADATA_KEY_COMPRESSION = "KBC.datatype.compression";
    const KBC_METADATA_KEY_FORMAT = "KBC.datatype.format";

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
     * @param string|int|null $length
     * @return bool
     */
    protected function isEmpty($length)
    {
        return $length === null || $length === '';
    }

    /**
     * @return array
     */
    public function toMetadata()
    {
        $metadata = [
            [
                "key" => self::KBC_METADATA_KEY_TYPE,
                "value" => $this->getType(),
            ],[
                "key" => self::KBC_METADATA_KEY_NULLABLE,
                "value" => $this->isNullable()
            ],[
                "key" => self::KBC_METADATA_KEY_BASETYPE,
                "value" => $this->getBasetype()
            ]
        ];
        if ($this->getLength()) {
            $metadata[] = [
                "key" => self::KBC_METADATA_KEY_LENGTH,
                "value" => $this->getLength()
            ];
        }
        if (!is_null($this->getDefault())) {
            $metadata[] = [
                "key" => self::KBC_METADATA_KEY_DEFAULT,
                "value" => $this->getDefault(),
            ];
        }
        return $metadata;
    }

    /**
     * @param string|null $length
     * @param int $firstMax
     * @param int $secondMax
     * @param bool $firstMustBeBigger
     * @return bool
     */
    protected function validateNumericLength($length, $firstMax, $secondMax, $firstMustBeBigger = true)
    {
        if ($this->isEmpty($length)) {
            return true;
        }
        $parts = explode(',', (string) $length);
        if (!in_array(count($parts), [1, 2])) {
            return false;
        }
        if (!is_numeric($parts[0])) {
            return false;
        }
        if (isset($parts[1]) && !is_numeric($parts[1])) {
            return false;
        }
        if ((int) $parts[0] <= 0 || (int) $parts[0] > $firstMax) {
            return false;
        }
        if (isset($parts[1]) && ((int) $parts[1] > $secondMax)) {
            return false;
        }

        if ($firstMustBeBigger && isset($parts[1]) && (int) $parts[1] > (int) $parts[0]) {
            return false;
        }
        return true;
    }

    /**
     * @param string|int|null $length
     * @param int $max
     * @param int $min
     * @return bool
     */
    protected function validateMaxLength($length, $max, $min = 1)
    {
        if ($this->isEmpty($length)) {
            return true;
        }

        if (!is_numeric($length)) {
            return false;
        }
        if ((int) $length < $min || (int) $length > $max) {
            return false;
        }
        return true;
    }
}
