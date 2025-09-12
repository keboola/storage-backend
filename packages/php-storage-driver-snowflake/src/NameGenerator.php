<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake;

use InvalidArgumentException;
use Keboola\Snowflake\Exception;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\InvalidObjectNameException;
use Keboola\Validate\Snowflake\ObjectNameValidate as SnowflakeObjectNameValidate;

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

    public function createObjectNameForBucketInProject(
        string $bucketId,
        bool $isBranchDefault,
        ?string $branchId,
    ): string {
        if ($isBranchDefault) {
            return $bucketId;
        }
        if ($branchId === null || $branchId === '') {
            throw new InvalidArgumentException('Branch ID must be provided for dev branch.');
        }
        return sprintf('%s_%s', $branchId, $bucketId);
    }

    public function createReadOnlyRoleNameForProject(string $projectId): string
    {
        return strtoupper(sprintf(
            '%s_%s_RO',
            rtrim($this->stackPrefix, '_'),
            $projectId,
        ));
    }

    public function defaultNetworkPolicyName(): string
    {
        return strtoupper(sprintf(
            '%s_SYSTEM_IPS_ONLY',
            rtrim($this->stackPrefix, '_'),
        ));
    }

    public function createUserNameForProject(string $projectId): string
    {
        $name = sprintf('%s%s', $this->stackPrefix, $projectId);
        if (!$this->isValidSnowflakeObjectName($name)) {
            throw new InvalidObjectNameException($name);
        }
        return strtoupper($name);
    }

    /**
     * this function is rewrite of ObjectNameValidate from Connection
     */
    private function isValidSnowflakeObjectName(string $name): bool
    {
        $maxLength = 256;
        $length = strlen($name);

        if ($length < 1 || $length > $maxLength) {
            return false;
        }

        // ASCII 32 (space) to 126 (~)
        if (!preg_match('/^[\x20-\x7E]+$/', $name)) {
            return false;
        }

        return true;
    }
}
