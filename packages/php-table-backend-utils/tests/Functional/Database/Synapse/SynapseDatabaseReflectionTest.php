<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Database\Synapse;

use Keboola\TableBackendUtils\Database\SynapseDatabaseReflection;
use Tests\Keboola\TableBackendUtils\Functional\Auth\BaseAuthTestCase;

class SynapseDatabaseReflectionTest extends BaseAuthTestCase
{
    private const LOGIN_PREFIX = 'UTILS_TEST_DATABASE_';

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpUser(self::LOGIN_PREFIX);
    }

    public function testGetRolesNames(): void
    {
        $this->dropRoles(self::LOGIN_PREFIX);
        $roleName = $this->currentLogin . '_ROLE';

        $this->connection->exec(sprintf(
            'CREATE ROLE %s',
            $this->platform->quoteSingleIdentifier($this->currentLogin . '_ROLE')
        ));
        $ref = new SynapseDatabaseReflection($this->connection);
        $names = $ref->getRolesNames(self::LOGIN_PREFIX . '%');
        $this->assertCount(1, $names);

        $names = $ref->getRolesNames($roleName);
        $this->assertSame([$roleName], $names);

        $names = $ref->getRolesNames();
        $this->assertGreaterThan(1, count($names));
    }

    public function testGetUsersNames(): void
    {
        $ref = new SynapseDatabaseReflection($this->connection);
        $names = $ref->getUsersNames(self::LOGIN_PREFIX . '%');
        $this->assertCount(1, $names);

        $names = $ref->getUsersNames($this->currentLogin);
        $this->assertSame([$this->currentLogin], $names);

        $names = $ref->getUsersNames();
        $this->assertGreaterThan(1, count($names));
    }
}
