<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportFunctional;

class CsvStub
{
    /** @var string[] */
    private array $columns;

    /** @var array<int,mixed> */
    private array $rows;

    /**
     * @param string[] $columns
     * @param array<int,mixed> $rows
     */
    public function __construct(array $columns, array $rows)
    {
        $this->columns = $columns;
        $this->rows = $rows;
    }

    /**
     * @return string[]
     */
    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * @return array<int,mixed>
     */
    public function getRows(): array
    {
        return $this->rows;
    }
}
