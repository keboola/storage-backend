<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\Snowflake;

use Keboola\Db\ImportExport\Backend\BackendImportAdapterInterface;
use Keboola\Db\ImportExport\Backend\ImporterInterface;
use Keboola\Db\ImportExport\Storage\NoBackendAdapterException;
use Keboola\Db\ImportExport\Storage\SourceInterface;

class SelectSource implements SourceInterface
{
    /**
     * @var string
     */
    private $query;

    /**
     * @var array
     */
    private $queryBindings;

    public function __construct(string $query, array $queryBindings = [])
    {
        $this->query = $query;
        $this->queryBindings = $queryBindings;
    }

    public function getBackendImportAdapter(
        ImporterInterface $importer
    ): BackendImportAdapterInterface {
        throw new NoBackendAdapterException();
    }

    public function getQuery(): string
    {
        return $this->query;
    }

    public function getQueryBindings(): array
    {
        return $this->queryBindings;
    }
}
