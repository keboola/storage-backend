<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

abstract class Common implements DefinitionInterface
{
    public const KBC_METADATA_KEY_BACKEND = 'KBC.datatype.backend';

    public const KBC_METADATA_KEY_TYPE = 'KBC.datatype.type';
    public const KBC_METADATA_KEY_NULLABLE = 'KBC.datatype.nullable';
    public const KBC_METADATA_KEY_BASETYPE = 'KBC.datatype.basetype';
    public const KBC_METADATA_KEY_LENGTH = 'KBC.datatype.length';
    public const KBC_METADATA_KEY_DEFAULT = 'KBC.datatype.default';

    public const KBC_METADATA_KEY_COMPRESSION = 'KBC.datatype.compression';
    public const KBC_METADATA_KEY_FORMAT = 'KBC.datatype.format';

    protected string $type;

    protected ?string $length = null;

    protected bool $nullable = true;

    protected ?string $default = null;

    /**
     * Common constructor.
     *
     * @param array{length?:string|null, nullable?:bool, default?:string|null} $options
     */
    public function __construct(string $type, array $options = [])
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

    public function getType(): string
    {
        return $this->type;
    }

    public function getLength(): ?string
    {
        return $this->length;
    }

    public function isNullable(): bool
    {
        return $this->nullable;
    }

    public function getDefault(): ?string
    {
        return $this->default;
    }

    abstract public function getSQLDefinition(): string;

    abstract public function getBasetype(): string;

    /**
     * @return array<mixed>
     */
    abstract public function toArray(): array;

    abstract public static function getTypeByBasetype(string $basetype): string;

    /**
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    protected function isEmpty($length): bool
    {
        return $length === null || $length === '';
    }

    /**
     * @return array<int, array{key:string,value:mixed}>
     */
    public function toMetadata(): array
    {
        $metadata = [
            [
                'key' => self::KBC_METADATA_KEY_TYPE,
                'value' => $this->getType(),
            ],[
                'key' => self::KBC_METADATA_KEY_NULLABLE,
                'value' => $this->isNullable(),
            ],[
                'key' => self::KBC_METADATA_KEY_BASETYPE,
                'value' => $this->getBasetype(),
            ],
        ];
        if ($this->getLength()) {
            $metadata[] = [
                'key' => self::KBC_METADATA_KEY_LENGTH,
                'value' => $this->getLength(),
            ];
        }
        if (!is_null($this->getDefault())) {
            $metadata[] = [
                'key' => self::KBC_METADATA_KEY_DEFAULT,
                'value' => $this->getDefault(),
            ];
        }
        return $metadata;
    }

    /**
     * @param null|int|string $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    protected function validateNumericLength(
        $length,
        int $firstMax,
        int $secondMax,
        bool $firstMustBeBigger = true
    ): bool {
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
        return !($firstMustBeBigger && isset($parts[1]) && (int) $parts[1] > (int) $parts[0]);
    }

    /**
     * @param string|int|null $length
     */
    //phpcs:ignore SlevomatCodingStandard.TypeHints.ParameterTypeHint.MissingNativeTypeHint
    protected function validateMaxLength($length, int $max, int $min = 1): bool
    {
        if ($this->isEmpty($length)) {
            return true;
        }

        if (!is_numeric($length)) {
            return false;
        }
        if (filter_var($length, FILTER_VALIDATE_INT) === false) {
            return false;
        }
        return (int) $length >= $min && (int) $length <= $max;
    }
}
