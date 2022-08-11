<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Teradata;

use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Teradata;
use Keboola\TableBackendUtils\Column\ColumnInterface;

final class TeradataColumn implements ColumnInterface
{
    private string $columnName;

    private Teradata $columnDefinition;

    public function __construct(string $columnName, Teradata $columnDefinition)
    {
        $this->columnName = $columnName;
        $this->columnDefinition = $columnDefinition;
    }

    public static function createGenericColumn(string $columnName): TeradataColumn
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

    public static function createTimestampColumn(string $columnName = self::TIMESTAMP_COLUMN_NAME): TeradataColumn
    {
        return new self(
            $columnName,
            new Teradata(
                Teradata::TYPE_TIMESTAMP,
            )
        );
    }
}
