<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Exasol;

use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class SelectSource implements SourceInterface, SqlSourceInterface
{
    /** @var string */
    private $query;

    /** @var string[] */
    private $queryBindings;

    /** @var string[] */
    private $dataTypes;

    /** @var string[] */
    private $columnsNames;

    /** @var string[]|null */
    private $primaryKeysNames;

    /**
     * @param string $query
     * @param string[] $queryBindings
     * @param string[] $dataTypes
     * @param string[] $columnsNames
     * @param string[]|null $primaryKeysNames
     */
    public function __construct(
        string $query,
        array $queryBindings = [],
        array $dataTypes = [],
        array $columnsNames = [],
        ?array $primaryKeysNames = null
    ) {
        $this->query = $query;
        $this->queryBindings = $queryBindings;
        $this->dataTypes = $dataTypes;
        $this->columnsNames = $columnsNames;
        $this->primaryKeysNames = $primaryKeysNames;
    }

    /**
     * @return string[]
     */
    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

    /**
     * @return string[]
     */
    public function getDataTypes(): array
    {
        return $this->dataTypes;
    }

    public function getFromStatement(): string
    {
        return sprintf('%s', $this->getQuery());
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    /**
     * @return string[]|null
     */
    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    /**
     * @return array|string[]
     */
    public function getQueryBindings(): array
    {
        return $this->queryBindings;
    }
}
