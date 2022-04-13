<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata\Database;

use Keboola\TableBackendUtils\Database\Teradata\TeradataDatabaseReflection;
use Tests\Keboola\TableBackendUtils\Functional\Teradata\TeradataBaseCase;

class TeradataDatabaseReflectionTest extends TeradataBaseCase
{
    private const USERNAME = 'frantaOmacka';
    private const ROLE = 'superMegaAdmin';
    private const USERNAME_PREFIX = 'franta';
    private const ROLE_PREFIX = 'super';

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpUser(self::USERNAME);
        $this->setUpRole(self::ROLE);
    }

    public function testGetRoles(): void
    {
        $ref = new TeradataDatabaseReflection($this->connection);
        $names = $ref->getRolesNames();
        self::assertContains(self::ROLE, $names);
    }

    public function testGetRolesWithLike(): void
    {
        $ref = new TeradataDatabaseReflection($this->connection);
        $names = $ref->getRolesNames(self::ROLE_PREFIX);
        self::assertContains(self::ROLE, $names);
    }

    public function testGetUsersNames(): void
    {
        $ref = new TeradataDatabaseReflection($this->connection);
        $names = $ref->getUsersNames();
        self::assertContains(self::USERNAME, $names);
    }

    public function testGetUsersNamesWithLike(): void
    {
        $ref = new TeradataDatabaseReflection($this->connection);
        $names = $ref->getUsersNames(self::USERNAME_PREFIX);
        self::assertContains(self::USERNAME, $names);
    }
}
