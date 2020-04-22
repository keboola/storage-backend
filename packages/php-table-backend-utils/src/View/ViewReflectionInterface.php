<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View;

interface ViewReflectionInterface
{
    public function getDependentViews(): array;
}
