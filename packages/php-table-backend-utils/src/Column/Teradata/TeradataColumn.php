<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Teradata;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnInterface;

final class TeradataColumn implements ColumnInterface
{
    private string $columnName;

    private \Keboola\Datatype\Definition\Teradata $columnDefinition;

    public function __construct(string $columnName, Teradata $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    /**
     * @return TeradataColumn
     */
    public static function createGenericColumn(string $columnName): ColumnInterface
    {
        $definition = new Teradata(
            Teradata::TYPE_VARCHAR,
            [
                'length' => '32000', // max value is 32k for UNICODE,GRAPHIC,KANJISJIS, 64k for LATIN
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
     * @return Teradata
     */
    public function getColumnDefinition(): DefinitionInterface
    {
        return $this->columnDefinition;
    }

    /**
     * @inheritDoc
     */
    public static function createFromDB(array $dbResponse): TeradataColumn
    {
        // TODO: Implement createFromDB() method.
        return self::createGenericColumn('tmp');
    }
}
