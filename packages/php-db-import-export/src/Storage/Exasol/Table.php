<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Exasol;

use Keboola\Db\ImportExport\Storage\DestinationInterface;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;
use Keboola\TableBackendUtils\Escaping\Exasol\ExasolQuote;

class Table implements SourceInterface, DestinationInterface, SqlSourceInterface
{
    private string $schema;

    private string $tableName;

    /** @var string[] */
    private array $columnsNames;

    /** @var string[]|null */
    private ?array $primaryKeysNames = null;

    /**
     * @param string[] $columns
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $schema,
        string $tableName,
        array $columns = [],
        ?array $primaryKeysNames = null
    ) {
        $this->schema = $schema;
        $this->tableName = $tableName;
        $this->columnsNames = $columns;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    public function getFromStatement(): string
    {
        $select = '*';
        $colums = $this->getColumnsNames();
        if ($colums !== []) {
            $quotedColumns = array_map(static function ($column) {
                return ExasolQuote::quoteSingleIdentifier($column);
            }, $colums);
            $select = implode(', ', $quotedColumns);
        }

        return sprintf('SELECT %s FROM %s', $select, $this->getQuotedTableWithScheme());
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    public function getQuotedTableWithScheme(): string
    {
        return sprintf(
            '%s.%s',
            ExasolQuote::quoteSingleIdentifier($this->getSchema()),
            ExasolQuote::quoteSingleIdentifier($this->getTableName())
        );
    }

    /** @return string[] */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /**
     * @return string[]
     */
    public function getQueryBindings(): array
    {
        // TODO
        return [];
    }

    public function getSchema(): string
    {
        return $this->schema;
    }

    public function getTableName(): string
    {
        return $this->tableName;
    }
}
