<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    /** @var string */
    private $schema;

    /** @var string */
    private $tableName;

    /** @var string[] */
    private $columnsNames;

    /**
     * @param string[] $columnsNames
     */
    public function __construct(string $schema, string $tableName, array $columnsNames = [])
    {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->columnsNames = $columnsNames;
    }

    public function getFromStatement(): string
    {
        $quotedColumns = array_map(static function ($column) {
            return QuoteHelper::quoteIdentifier($column);
        }, $this->getColumnsNames());

        $select = '*';
        if (count($quotedColumns) > 0) {
            $select = implode(', ', $quotedColumns);
        }

        return sprintf(
            'SELECT %s FROM %s.%s',
            $select,
            QuoteHelper::quoteIdentifier($this->getSchema()),
            QuoteHelper::quoteIdentifier($this->getTableName())
        );
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            QuoteHelper::quoteIdentifier($this->schema),
            QuoteHelper::quoteIdentifier($this->tableName)
        );
    }

    public function getQueryBindings(): array
    {
        return [];
    }
}
