<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake\Database;

use Keboola\TableBackendUtils\Database\Snowflake\SnowflakeDatabaseReflection;
use Tests\Keboola\TableBackendUtils\Functional\Snowflake\SnowflakeBaseCase;

class SnowflakeDatabaseReflectionTest extends SnowflakeBaseCase
{
    private const USERNAME = 'FRANTA_OMACKA';
    private const ROLE = 'SUPER_MEGA_ADMIN';
    private const USERNAME_PREFIX = 'FRANTA';
    private const ROLE_PREFIX = 'SUPER';

    protected function setUp(): void
    {
        $this->markTestSkipped('Test won\'t work as testing user has not such high privileges.');
        /** @phpstan-ignore-next-line */
        parent::setUp();
        $this->setUpUser(self::USERNAME);
        $this->setUpRole(self::ROLE);
    }

    public function testGetRoles(): void
    {
        $ref = new SnowflakeDatabaseReflection($this->connection);
        $names = $ref->getRolesNames();
        self::assertContains(self::ROLE, $names);
    }

    public function testGetRolesWithLike(): void
    {
        $ref = new SnowflakeDatabaseReflection($this->connection);
        $names = $ref->getRolesNames(self::ROLE_PREFIX);
        self::assertContains(self::ROLE, $names);
    }

    public function testGetUsersNames(): void
    {
        $ref = new SnowflakeDatabaseReflection($this->connection);
        $names = $ref->getUsersNames();
        self::assertContains(self::USERNAME, $names);
    }

    public function testGetUsersNamesWithLike(): void
    {
        $ref = new SnowflakeDatabaseReflection($this->connection);
        $names = $ref->getUsersNames(self::USERNAME_PREFIX);
        self::assertContains(self::USERNAME, $names);
    }
}
