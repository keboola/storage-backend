<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

interface SourceInterface
{
    /**
     * @return string[]
     */
    public function getColumnsNames(): array;
}
