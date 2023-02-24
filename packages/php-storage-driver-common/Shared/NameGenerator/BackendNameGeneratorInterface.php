<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\NameGenerator;

interface BackendNameGeneratorInterface
{
    public function createUserNameForProject(string $projectId): string;

    public function createRoleNameForProject(string $projectId): string;

    public function createObjectNameForBucketInProject(string $bucketId, string $projectId): string;

    public function createWorkspaceUserNameForWorkspaceId(string $workspaceId): string;

    public function createWorkspaceRoleNameForWorkspaceId(string $workspaceId): string;

    public function createWorkspaceObjectNameForWorkspaceId(string $workspaceId): string;

    public function createReadOnlyRoleNameForProject(string $projectId): string;

    public function createShareRoleNameForBucket(string $projectId, string $bucketId): string;
}
