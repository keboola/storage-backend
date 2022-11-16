<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth;

interface UserReflectionInterface
{
    public function endAllSessions(): void;

    /**
     * @return string[]
     */
    public function getAllSessionIds(): array;
}
