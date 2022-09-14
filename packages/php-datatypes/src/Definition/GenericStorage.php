<?php

declare(strict_types=1);

namespace Keboola\Datatype\Definition;

use LogicException;

class GenericStorage extends Common
{
    public const DATE_TYPES = ['date'];
    public const TIMESTAMP_TYPES = [
        'datetime',
        'datetime2',
        'smalldatetime',
        'datetimeoffset',
        'timestamp_LTZ',
        'timestamp_NTZ',
        'TIMESTAMP_TZ',
        'timestamptz',
        'timestamp',
        'timestamp with time zone',
        'timestamp with local time zone',
        'timestamp without time zone',
    ];
    public const FLOATING_POINT_TYPES = [
        'real', 'float', 'float4', 'double precision', 'float8', 'binary_float', 'binary_double', 'double',
        'd_float', 'quad',
    ];
    // NOTE: "bit" is used in mssql as a 1/0 type boolean, but in pgsql as a bit(n) ie 10110.
    // also in mysql bit is equivalent to tinyint
    public const BOOLEAN_TYPES = ['boolean', 'bool'];

    public const INTEGER_TYPES = [
        'integer', 'int', 'smallint', 'mediumint',
        'int2', 'tinyint', 'bigint', 'int8', 'bigserial', 'serial8', 'int4', 'int64',
    ];

    public const FIXED_NUMERIC_TYPES = [
        'numeric', 'decimal', 'dec', 'fixed', 'money', 'smallmoney', 'number',
    ];

    protected ?string $format = null;

    /**
     * Base constructor.
     * @param array{length?:string|null, nullable?:bool, default?:string|null, format?:string|null} $options
     */
    public function __construct(string $type, array $options = [])
    {
        parent::__construct($type, $options);
        if (isset($options['format'])) {
            $this->format = $options['format'];
        }
    }

    public function getSQLDefinition(): string
    {
        $sql = $this->getType();
        if ($this->getLength() && $this->getLength() !== '') {
            $sql .= '(' . $this->getLength() . ')';
        }
        $sql .= ($this->isNullable()) ? ' NULL' : ' NOT NULL';
        if (!is_null($this->getDefault())) {
            $sql .= " DEFAULT '" . $this->default . "'";
        } elseif ($this->isNullable()) {
            $sql .= ' DEFAULT NULL';
        }
        return $sql;
    }

    /**
     * @return array<int, array{key:string,value:mixed}>
     */
    public function toMetadata(): array
    {
        $metadata = parent::toMetadata();
        if (!is_null($this->getFormat())) {
            $metadata[] = [
                'key' => Common::KBC_METADATA_KEY_FORMAT,
                'value' => $this->format,
            ];
        }
        return $metadata;
    }

    /**
     * @return array{type:string,length:string|null,nullable:bool,default?:string,format?:string}
     */
    public function toArray(): array
    {
        $result = [
            'type' => $this->getType(),
            'length' => $this->getLength(),
            'nullable' => $this->isNullable(),
        ];
        if (!is_null($this->getDefault())) {
            $result['default'] = $this->getDefault();
        }
        if ($this->getFormat()) {
            $result['format'] = $this->getFormat();
        }
        return $result;
    }

    public function getFormat(): ?string
    {
        return $this->format;
    }

    public function getBasetype(): string
    {
        $type = strtolower($this->type);
        $baseType = BaseType::STRING;
        if (in_array($type, self::DATE_TYPES)) {
            $baseType = BaseType::DATE;
        }
        if (in_array($type, self::TIMESTAMP_TYPES)) {
            $baseType = BaseType::TIMESTAMP;
        }
        if (in_array($type, self::INTEGER_TYPES)) {
            $baseType = BaseType::INTEGER;
        }
        if (in_array($type, self::FIXED_NUMERIC_TYPES)) {
            $baseType = BaseType::NUMERIC;
        }
        if (in_array($type, self::FLOATING_POINT_TYPES)) {
            $baseType = BaseType::FLOAT;
        }
        if (in_array($type, self::BOOLEAN_TYPES)) {
            $baseType = BaseType::BOOLEAN;
        }
        return $baseType;
    }

    public static function getTypeByBasetype(string $basetype): string
    {
        throw new LogicException('Method is not implemented yet.');
    }
}
