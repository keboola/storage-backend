<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\NameGenerator;

use Keboola\StorageDriver\Shared\BackendSupportsInterface;
use LogicException;

class GenericNameGenerator implements BackendNameGeneratorInterface, BackendSupportsInterface
{
    protected const ROLE_SUFFIX = '_ROLE';
    protected const USER_SUFFIX = '_USER';
    protected const READ_ONLY_ROLE_SUFFIX = '_RO';
    protected const SHARE_ROLE_SUFFIX = '_SHARE';

    protected string $stackPrefix;

    public function __construct(
        string $clientDbPrefix
    ) {
        if ($clientDbPrefix === '') {
            throw new LogicException('Client db prefix must be set');
        }
        $this->stackPrefix = $clientDbPrefix;
    }

    public function createUserNameForProject(string $projectId): string
    {
        return $this->createObjectNameForProject($projectId) . self::USER_SUFFIX;
    }

    private function createObjectNameForProject(string $projectId): string
    {
        return strtoupper($this->stackPrefix . $projectId);
    }

    public function createRoleNameForProject(string $projectId): string
    {
        return $this->createObjectNameForProject($projectId) . self::ROLE_SUFFIX;
    }

    public function createObjectNameForBucketInProject(string $bucketId, string $projectId): string
    {
        return str_replace(
            '.',
            '_',
            sprintf(
                '%s-%s',
                $this->createObjectNameForProject($projectId),
                $bucketId
            )
        );
    }

    public function createWorkspaceUserNameForWorkspaceId(string $workspaceId): string
    {
        return strtoupper($this->createWorkspaceCredentialsPrefix($workspaceId) . self::USER_SUFFIX);
    }

    private function createWorkspaceCredentialsPrefix(string $workspaceId): string
    {
        return $this->stackPrefix . 'workspace_' . $workspaceId;
    }

    public function createWorkspaceRoleNameForWorkspaceId(string $workspaceId): string
    {
        return strtoupper($this->createWorkspaceCredentialsPrefix($workspaceId) . self::ROLE_SUFFIX);
    }

    public function createWorkspaceObjectNameForWorkspaceId(string $workspaceId): string
    {
        return strtoupper('workspace_' . $workspaceId);
    }

    public function createReadOnlyRoleNameForProject(string $projectId): string
    {
        return $this->createObjectNameForProject($projectId) . self::READ_ONLY_ROLE_SUFFIX;
    }

    public function supportsBackend(string $backendName): bool
    {
        return in_array($backendName, self::SUPPORTED_BACKENDS);
    }

    public function createShareRoleNameForBucket(
        string $projectId,
        string $bucketId
    ): string {
        return strtoupper(sprintf(
            '%s_%s_%s%s',
            rtrim($this->stackPrefix, '_'),
            $projectId,
            $bucketId,
            self::SHARE_ROLE_SUFFIX
        ));
    }
}
