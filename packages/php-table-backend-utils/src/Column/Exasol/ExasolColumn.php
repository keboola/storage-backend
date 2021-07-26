<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Exasol;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Exasol;
use Keboola\TableBackendUtils\Column\ColumnInterface;

final class ExasolColumn implements ColumnInterface
{
    /** @var string */
    private $columnName;

    /** @var Exasol */
    private $columnDefinition;

    public function __construct(string $columnName, Exasol $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    /**
     * @param string $columnName
     * @return ExasolColumn
     */
    public static function createGenericColumn(string $columnName): ColumnInterface
    {
        $definition = new Exasol(
            Exasol::TYPE_VARCHAR,
            [
                'length' => '2000000',
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
     * @return Exasol
     */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }
}
