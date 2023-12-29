<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\ImportExport\Backend\Snowflake\Helper\QuoteHelper;
use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    private string $schema;

    private string $tableName;

    /** @var string[] */
    private array $columnsNames;

    /** @var string[]|null */
    private ?array $primaryKeysNames = null;

    /**
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $schema,
        string $tableName,
        array $columnsNames = [],
        ?array $primaryKeysNames = null,
    ) {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
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
            QuoteHelper::quoteIdentifier($this->getTableName()),
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

    /** @return string[]|null */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /** @return array<mixed> */
    public function getQueryBindings(): array
    {
        return [];
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            QuoteHelper::quoteIdentifier($this->schema),
            QuoteHelper::quoteIdentifier($this->tableName),
        );
    }

    public function getFromStatementWithStringCasting(): string
    {
        return $this->getFromStatement();
    }
}
