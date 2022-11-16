<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View;

interface ViewReflectionInterface
{
    /**
     * @return array{
     *  schema_name: string,
     *  name: string
     * }[]
     */
    public function getDependentViews(): array;
}
