<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Unit;

use InvalidArgumentException;
use Keboola\Connection\Storage\Service\Backend\NameGenerator\SnowflakeObjectNameGenerator;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use PHPUnit\Framework\TestCase;

class NameGeneratorTest extends TestCase
{
    private NameGenerator $nameGenerator;

    protected function setUp(): void
    {
        parent::setUp();
        $this->nameGenerator = new NameGenerator('KBC_prefix_');
    }

    public function testCreateReadOnlyRoleNameForBranch(): void
    {
        $projectId = $this->nameGenerator->createReadOnlyRoleNameForBranch('234', '567');
        $this->assertSame('KBC_PREFIX_234_567_RO', $projectId);
    }

    public function testCreateObjectNameForBucketInProject(): void
    {
        $this->assertSame(
            'in.c-bucketX',
            $this->nameGenerator->createObjectNameForBucketInProject(
                'in.c-bucketX',
                true,
                null,
            ),
        );
        $this->assertSame(
            'in.c-bucketX',
            $this->nameGenerator->createObjectNameForBucketInProject(
                'in.c-bucketX',
                true,
                '',
            ),
        );
        $this->assertSame(
            '123_in.c-bucketX',
            $this->nameGenerator->createObjectNameForBucketInProject(
                'in.c-bucketX',
                false,
                '123',
            ),
        );
    }

    public function testCreateObjectNameForBucketInProjectThrowsNullBranchIdOnDevBranch(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->assertSame(
            'in.c-bucketX',
            $this->nameGenerator->createObjectNameForBucketInProject(
                'in.c-bucketX',
                false,
                null,
            ),
        );
    }

    public function testCreateObjectNameForBucketInProjectThrowsEmptyBranchIdOnDevBranch(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->assertSame(
            'in.c-bucketX',
            $this->nameGenerator->createObjectNameForBucketInProject(
                'in.c-bucketX',
                false,
                '',
            ),
        );
    }

    public function testCreateUserNameForProject(): void
    {
        $objectName = $this->nameGenerator->createUserNameForProject('123456');
        $this->assertSame('KBC_PREFIX_123456', $objectName);
    }

    public function testCreateReadOnlyRoleNameForProject(): void
    {
        $objectName = $this->nameGenerator->createReadOnlyRoleNameForProject('123456');
        $this->assertSame('KBC_PREFIX_123456_RO', $objectName);
    }

    public function testDefaultNetworkPolicyName(): void
    {
        $objectName = $this->nameGenerator->defaultNetworkPolicyName();
        $this->assertSame('KBC_PREFIX_SYSTEM_IPS_ONLY', $objectName);
    }
}
