<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage;

interface SourceInterface
{
    /**
     * @return string[]
     */
    public function getColumnsNames(): array;

    /**
     * null means that primary keys are not known
     *
     * @return string[]|null
     */
    public function getPrimaryKeysNames(): ?array;
}
