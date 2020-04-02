<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Schema;

interface SchemaReflectionInterface
{
    /**
     * @return string[]
     */
    public function getTablesNames(): array;
}
