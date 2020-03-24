<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Synapse;

use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage\SourceInterface;
use Keboola\Db\ImportExport\Storage\SqlSourceInterface;

class SelectSource implements SourceInterface, SqlSourceInterface
{
    /**
     * @var string
     */
    private $query;

    /**
     * @var array
     */
    private $queryBindings;

    /** @var array */
    private $dataTypes;

    public function __construct(
        string $query,
        array $queryBindings = [],
        array $dataTypes = []
    ) {
        $this->query = $query;
        $this->queryBindings = $queryBindings;
        $this->dataTypes = $dataTypes;
    }

    public function getFromStatement(): string
    {
        return sprintf('%s', $this->getQuery());
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function getQueryBindings(): array
    {
        return $this->queryBindings;
    }

    public function getDataTypes(): array
    {
        return $this->dataTypes;
    }
}
