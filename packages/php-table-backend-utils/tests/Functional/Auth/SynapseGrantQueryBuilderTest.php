<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Auth;

use Generator;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOn;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOptions;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\Permission;
use Keboola\TableBackendUtils\Auth\SynapseGrantQueryBuilder;
use Tests\Keboola\TableBackendUtils\Functional\SynapseBaseCase;

class SynapseGrantQueryBuilderTest extends SynapseUserReflectionTest
{
    protected const LOGIN_PREFIX = 'UTILS_TEST_GRANT_LOGIN_';
    private const TEST_SCHEMA = 'UTILS_TEST_GRANT_SCHEMA';
    private const TEST_TABLE = 'UTILS_TEST_GRANT_TABLE';

    protected function setUp(): void
    {
        SynapseBaseCase::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->dropRoles();
        $this->setUpUser();

        $this->connection->exec(sprintf(
            'CREATE ROLE %s',
            $this->platform->quoteSingleIdentifier($this->currentLogin . '_ROLE')
        ));

        $this->connection->exec(sprintf(
            'CREATE SCHEMA %s AUTHORIZATION %s',
            $this->platform->quoteSingleIdentifier(self::TEST_SCHEMA),
            $this->platform->quoteSingleIdentifier($this->currentLogin)
        ));

        $this->connection->exec(sprintf(
            'CREATE TABLE %s.%s ([col1] nvarchar(4000) NOT NULL DEFAULT \'\')',
            $this->platform->quoteSingleIdentifier(self::TEST_SCHEMA),
            $this->platform->quoteSingleIdentifier(self::TEST_TABLE)
        ));
    }

    protected function dropRoles(): void
    {
        // drop all roles
        $roles = $this->connection->fetchAll(sprintf(
            'SELECT [name] FROM [sys].[sysusers] WHERE [name] LIKE N%s AND [issqlrole] = 1',
            $this->connection->quote(self::LOGIN_PREFIX . '%')
        ));
        foreach ($roles as $role) {
            $this->connection->exec(sprintf(
                'DROP ROLE %s',
                $this->platform->quoteSingleIdentifier($role['name'])
            ));
        }
    }

    /**
     * @return Generator<array{
     *     array<Permission::GRANT_*>,
     *     null|GrantOn::ON_*,
     *     string[],
     *     bool,
     *     bool,
     *     bool,
     *     string
     * }>
     */
    public function grantDataProvider(): Generator
    {
        $grantsWithoutOn = [
            Permission::GRANT_ALTER_ANY_EXTERNAL_DATA_SOURCE,
            Permission::GRANT_ALTER_ANY_EXTERNAL_FILE_FORMAT,
            Permission::GRANT_ADMINISTER_DATABASE_BULK_OPERATIONS,
            Permission::GRANT_CONTROL,
            Permission::GRANT_ALTER,
            Permission::GRANT_VIEW_DEFINITION,
            Permission::GRANT_TAKE_OWNERSHIP,
            Permission::GRANT_ALTER_ANY_DATASPACE,
            Permission::GRANT_ALTER_ANY_ROLE,
            Permission::GRANT_ALTER_ANY_SCHEMA,
            Permission::GRANT_ALTER_ANY_USER,
            Permission::GRANT_BACKUP_DATABASE,
            Permission::GRANT_CREATE_PROCEDURE,
            Permission::GRANT_CREATE_ROLE,
            Permission::GRANT_CREATE_SCHEMA,
            Permission::GRANT_CREATE_TABLE,
            Permission::GRANT_CREATE_VIEW,
            Permission::GRANT_SHOWPLAN,
            Permission::GRANT_DELETE,
            Permission::GRANT_EXECUTE,
            Permission::GRANT_INSERT,
            Permission::GRANT_SELECT,
            Permission::GRANT_UPDATE,
            Permission::GRANT_REFERENCES,
        ];
        yield 'grant without on to user' => [
            $grantsWithoutOn,
            null,
            [],
            false,
            false,
            false,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s]',
        ];

        yield 'grant without on to user with grant option' => [
            $grantsWithoutOn,
            null,
            [],
            false,
            true,
            false,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s] WITH GRANT OPTION',
        ];

        yield 'grant without on to role' => [
            $grantsWithoutOn,
            null,
            [],
            false,
            false,
            true,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s_ROLE]',
        ];

        yield 'grant without on to role with grant option' => [
            $grantsWithoutOn,
            null,
            [],
            false,
            true,
            true,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s_ROLE] WITH GRANT OPTION',
        ];

        $grantsOnSchema = [
            Permission::GRANT_CONTROL,
            Permission::GRANT_ALTER,
            Permission::GRANT_VIEW_DEFINITION,
            Permission::GRANT_TAKE_OWNERSHIP,
            Permission::GRANT_DELETE,
            Permission::GRANT_EXECUTE,
            Permission::GRANT_INSERT,
            Permission::GRANT_SELECT,
            Permission::GRANT_UPDATE,
            Permission::GRANT_REFERENCES,
        ];

        yield 'grant on schema to user' => [
            $grantsOnSchema,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            false,
            false,
// phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s]',
        ];

        yield 'grant on schema to user with grant option' => [
            $grantsOnSchema,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            true,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s] WITH GRANT OPTION',
        ];

        yield 'grant on schema to role' => [
            $grantsOnSchema,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            false,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s_ROLE]',
        ];

        yield 'grant on schema to role with grant option' => [
            $grantsOnSchema,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            true,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s_ROLE] WITH GRANT OPTION',
        ];

        $grantsOnUser = [
            Permission::GRANT_CONTROL,
            Permission::GRANT_ALTER,
            Permission::GRANT_VIEW_DEFINITION,
        ];

        yield 'grant on user to user' => [
            $grantsOnUser,
            GrantOn::ON_USER,
            [],
            true,
            false,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s]',
        ];

        yield 'grant on user to user with grant option' => [
            $grantsOnUser,
            GrantOn::ON_USER,
            [],
            true,
            true,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s] WITH GRANT OPTION',
        ];

        yield 'grant on user to role' => [
            $grantsOnUser,
            GrantOn::ON_USER,
            [],
            true,
            false,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s_ROLE]',
        ];

        yield 'grant on user to role with grant option' => [
            $grantsOnUser,
            GrantOn::ON_USER,
            [],
            true,
            true,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s_ROLE] WITH GRANT OPTION',
        ];

        $grantsOnTable = [
            Permission::GRANT_CONTROL,
            Permission::GRANT_ALTER,
            Permission::GRANT_VIEW_DEFINITION,
            Permission::GRANT_TAKE_OWNERSHIP,
            Permission::GRANT_INSERT,
            Permission::GRANT_SELECT,
            Permission::GRANT_UPDATE,
            Permission::GRANT_REFERENCES,
        ];
        yield 'grant on table' => [
            $grantsOnTable,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            false,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s]',
        ];
        yield 'grant on table to role' => [
            $grantsOnTable,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            false,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s_ROLE]',
        ];
        yield 'grant on table with grant option' => [
            $grantsOnTable,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            true,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s] WITH GRANT OPTION',
        ];
        yield 'grant on table to role with grant option' => [
            $grantsOnTable,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            true,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s_ROLE] WITH GRANT OPTION',
        ];
    }

    /**
     * @param array<Permission::GRANT_*> $permissions
     * @param null|GrantOn::ON_* $grantSubject
     * @param string[] $grantOnTargetPath
     * @throws \Doctrine\DBAL\DBALException

     * @dataProvider grantDataProvider
     */
    public function testGrant(
        array $permissions,
        ?string $grantSubject,
        array $grantOnTargetPath,
        bool $useCurrentLoginOnEndPath,
        bool $allowGrantOption,
        bool $grantToRole,
        string $expected
    ): void {
        if ($useCurrentLoginOnEndPath === true) {
            $grantOnTargetPath[] = $this->currentLogin;
        }
        $options = new GrantOptions($allowGrantOption);

        $qb = new SynapseGrantQueryBuilder($this->connection);
        $sql = $qb->getGrantSql(
            $permissions,
            $grantSubject,
            $grantOnTargetPath,
            $this->currentLogin . ($grantToRole === true ? '_ROLE' : ''),
            $options
        );
        $this->connection->exec($sql);
        $this->assertSame(sprintf($expected, $this->currentLogin, $this->currentLogin), $sql);
    }
}
