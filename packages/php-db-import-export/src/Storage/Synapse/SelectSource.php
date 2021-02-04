<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class SelectSource implements SourceInterface, SqlSourceInterface
{
    /** @var string */
    private $query;

    /** @var array */
    private $queryBindings;

    /** @var array */
    private $dataTypes;

    /** @var string[] */
    private $columnsNames;

    /** @var string[]|null */
    private $primaryKeysNames;

    /**
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

    public function getColumnsNames(): array
    {
        return $this->columnsNames;
    }

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

    public function getPrimaryKeysNames(): ?array
    {
        return $this->primaryKeysNames;
    }

    public function getQueryBindings(): array
    {
        return $this->queryBindings;
    }
}
