<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake;

class NameGenerator
{
    public function __construct(
        private readonly string $stackPrefix,
    ) {
    }

    public function createReadOnlyRoleNameForBranch(string $projectId, string $branchId): string
    {
        return strtoupper(sprintf(
            '%s_%s_%s_RO',
            rtrim($this->stackPrefix, '_'),
            $projectId,
            $branchId,
        ));
    }
}
