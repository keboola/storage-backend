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

    public function __construct(
        ?string $query = null,
        bool $isCompresed = false
    ) {
        $this->query = $query;
        $this->isCompresed = $isCompresed;
    }

    public function getQuery(): ?string
    {
        return $this->query;
    }

    public function isCompresed(): bool
    {
        return $this->isCompresed;
    }
}
