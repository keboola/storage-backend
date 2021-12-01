<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Snowflake;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\TableBackendUtils\Column\ColumnInterface;

final class SnowflakeColumn implements ColumnInterface
{
    /** @var string */
    private $columnName;

    /** @var Snowflake */
    private $columnDefinition;

    public function __construct(string $columnName, Snowflake $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    /**
     * @param string $columnName
     * @return SnowflakeColumn
     */
    public static function createGenericColumn(string $columnName): ColumnInterface
    {
        $definition = new Snowflake(
            Snowflake::TYPE_VARCHAR,
            [
                'nullable' => false,
                'default' => '\'\'',
            ]
        );

        return new self(
            $columnName,
            $definition
        );
    }

    public function getColumnName(): string
    {
        return $this->columnName;
    }

    /**
     * @return Snowflake
     */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }

    public static function createFromDB(array $column): ColumnInterface
    {
        $type = $column['type'];
        $default = $column['default'];
        $length = null;

        $matches = [];
        if (preg_match('/^(\w+)\(([0-9\,]+)\)$/ui', $column['type'], $matches)) {
            $type = $matches[1];
            $length = $matches[2];
        }

        return new self($column['name'], new Snowflake(
            $type,
            [
                'nullable' => $column['null?'] === 'Y' ? true : false,
                'length' => $length,
                'default' => $default,
            ]
        ));
    }
}
