<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Database;

interface DatabaseReflectionInterface
{
    /**
     * @return string[]
     */
    public function getUsersNames(?string $like = null): array;

    /**
     * @return string[]
     */
    public function getRolesNames(?string $like = null): array;
}
