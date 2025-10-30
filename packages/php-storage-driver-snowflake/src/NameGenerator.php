<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake;

use InvalidArgumentException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\InvalidObjectNameException;

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
            $this->getStackPrefix(),
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
            $this->getStackPrefix(),
            $projectId,
        ));
    }

    public function defaultNetworkPolicyName(): string
    {
        return strtoupper(sprintf(
            '%s_SYSTEM_IPS_ONLY',
            $this->getStackPrefix(),
        ));
    }

    public function createUserNameForProject(string $projectId): string
    {
        $name = sprintf(
            '%s_%s',
            $this->getStackPrefix(),
            $projectId,
        );
        if (!$this->isValidSnowflakeObjectName($name)) {
            throw new InvalidObjectNameException($name);
        }
        return strtoupper($name);
    }

    public function createWorkspaceSchemaName(
        string $workspaceId,
        bool $isBranchDefault,
        string $branchId,
    ): string {
        $devBranchPrefix = $this->getDevBranchPrefix($isBranchDefault, $branchId);
        $workspaceName = $devBranchPrefix . 'workspace_' . $workspaceId;
        return strtoupper($workspaceName);
    }

    public function createWorkspaceRoleName(
        string $workspaceId,
        bool $isBranchDefault,
        string $branchId,
    ): string {
        return $this->createWorkspaceUserName($workspaceId, $isBranchDefault, $branchId);
    }

    public function createWorkspaceUserName(
        string $workspaceId,
        bool $isBranchDefault,
        string $branchId,
    ): string {
        $devBranchPrefix = $this->getDevBranchPrefix($isBranchDefault, $branchId);
        $credentialsName = $this->stackPrefix . $devBranchPrefix . 'workspace_' . $workspaceId;
        return strtoupper($credentialsName);
    }

    private function getDevBranchPrefix(bool $isDefaultBranch, string $devBranchId): string
    {
        $workspacePrefix = '';
        if (!$isDefaultBranch) {
            $workspacePrefix = $devBranchId . '_';
        }
        return $workspacePrefix;
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

    private function getStackPrefix(): string
    {
        return rtrim($this->stackPrefix, '_');
    }
}
