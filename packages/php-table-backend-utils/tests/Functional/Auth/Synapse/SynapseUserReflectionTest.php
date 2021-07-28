<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Auth\Synapse;

use Doctrine\DBAL\DBALException;
use Keboola\TableBackendUtils\Auth\SynapseUserReflection;
use Tests\Keboola\TableBackendUtils\Functional\Auth\BaseAuthTestCase;

class SynapseUserReflectionTest extends BaseAuthTestCase
{
    private const LOGIN_PREFIX = 'UTILS_TEST_AUTH_LOGIN_';

    /**
     * @var string
     *
     * User name has to be generated because when new user is created
     * deleted and created it has no access to db :/
     * there is some time to propagate these changes and test are too fast
     */
    protected $currentLogin;

    protected function setUp(): void
    {
        parent::setUp();
        $this->setUpUser(self::LOGIN_PREFIX);
    }

    public function testEndAllUserSessions(): void
    {
        $ref = new SynapseUserReflection($this->connection, $this->currentLogin);
        $this->assertCount(0, $ref->getAllSessionIds());

        // connect as user
        $dbUser = $this->getTestLoginConnection();
        $dbUser->connect();

        $ref = new SynapseUserReflection($this->connection, $this->currentLogin);

        $this->assertGreaterThan(0, count($ref->getAllSessionIds()));

        $dbUser->fetchAll('SELECT * FROM sys.tables');

        $ref->endAllSessions();

        $this->expectException(DBALException::class);
        $dbUser->fetchAll('SELECT * FROM sys.tables');
    }
}
