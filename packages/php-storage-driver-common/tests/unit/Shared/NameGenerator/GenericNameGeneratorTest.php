<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\NameGenerator;

use Keboola\StorageDriver\Shared\NameGenerator\GenericNameGenerator;
use PHPUnit\Framework\TestCase;

class GenericNameGeneratorTest extends TestCase
{
    protected const TEST_CLIENT_DB_PREFIX = 'my_test_prefix_';

    public function testCreateUserNameForProject(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);

        $userName = $generator->createUserNameForProject('123');
        $this->assertSame('MY_TEST_PREFIX_123_USER', $userName);
    }

    public function testCreateRoleNameForProject(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);

        $roleName = $generator->createRoleNameForProject('123');
        $this->assertSame('MY_TEST_PREFIX_123_ROLE', $roleName);
    }

    public function testCreateReadOnlyRoleNameForProject(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);

        $roleName = $generator->createReadOnlyRoleNameForProject('123');
        $this->assertSame('MY_TEST_PREFIX_123_RO', $roleName);
    }

    public function testCreateObjectNameForBucket(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);
        $schemaName = $generator->createObjectNameForBucketInProject('in.c-forever', '123');

        $this->assertSame('MY_TEST_PREFIX_123-in_c-forever', $schemaName);
    }

    public function testCreateWorkspaceUserNameForWorkspaceId(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);
        $objectName = $generator->createWorkspaceUserNameForWorkspaceId('123456');
        $this->assertSame('MY_TEST_PREFIX_WORKSPACE_123456_USER', $objectName);
    }

    public function testCreateWorkspaceRoleNameForWorkspaceId(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);
        $objectName = $generator->createWorkspaceRoleNameForWorkspaceId('123456');
        $this->assertSame('MY_TEST_PREFIX_WORKSPACE_123456_ROLE', $objectName);
    }

    public function testCreateWorkspaceObjectNameForWorkspaceId(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);
        $objectName = $generator->createWorkspaceObjectNameForWorkspaceId('123456');
        $this->assertSame('WORKSPACE_123456', $objectName);
    }

    public function testCreateShareRoleNameForBucket(): void
    {
        $generator = new GenericNameGenerator(self::TEST_CLIENT_DB_PREFIX);
        $objectName = $generator->createShareRoleNameForBucket('123456', 'bucketABC');
        $this->assertSame('MY_TEST_PREFIX_123456_BUCKETABC_SHARE', $objectName);
    }
}
