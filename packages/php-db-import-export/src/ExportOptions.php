<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport;

class ExportOptions
{
    /**
     * @var string|null
     */
    private $query;

    /**
     * @var bool
     */
    private $isCompresed;

    /**
     * @var array
     */
    private $queryBinding;

    public function __construct(
        ?string $query = null,
        array $queryBindings = [],
        bool $isCompresed = false
    ) {
        $this->query = $query;
        $this->isCompresed = $isCompresed;
        $this->queryBinding = $queryBindings;
    }

    public function getQuery(): ?string
    {
        return $this->query;
    }

    public function getQueryBindings(): array
    {
        return $this->queryBinding;
    }

    public function isCompresed(): bool
    {
        return $this->isCompresed;
    }
}
