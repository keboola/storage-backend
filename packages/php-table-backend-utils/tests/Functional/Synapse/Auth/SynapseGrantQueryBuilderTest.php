<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Synapse\Auth;

use Generator;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOn;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\GrantOptions;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\Permission;
use Keboola\TableBackendUtils\Auth\Grant\Synapse\RevokeOptions;
use Keboola\TableBackendUtils\Auth\SynapseGrantQueryBuilder;

class SynapseGrantQueryBuilderTest extends BaseAuthTestCase
{
    private const LOGIN_PREFIX = 'UTILS_TEST_GRANT_LOGIN_';
    private const TEST_SCHEMA = 'UTILS_TEST_GRANT_SCHEMA';
    private const TEST_TABLE = 'UTILS_TEST_GRANT_TABLE';
    // permissions groups
    private const TEST_GRANTS_WITHOUT_ON = [
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
    private const TEST_GRANTS_ON_SCHEMA = [
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
    private const TEST_GRANTS_ON_USER = [
        Permission::GRANT_CONTROL,
        Permission::GRANT_ALTER,
        Permission::GRANT_VIEW_DEFINITION,
    ];
    private const TEST_GRANTS_ON_TABLE = [
        Permission::GRANT_CONTROL,
        Permission::GRANT_ALTER,
        Permission::GRANT_VIEW_DEFINITION,
        Permission::GRANT_TAKE_OWNERSHIP,
        Permission::GRANT_INSERT,
        Permission::GRANT_SELECT,
        Permission::GRANT_UPDATE,
        Permission::GRANT_REFERENCES,
    ];

    protected function setUp(): void
    {
        parent::setUp();
        $this->dropAllWithinSchema(self::TEST_SCHEMA);
        $this->dropRoles(self::LOGIN_PREFIX);
        $this->setUpUser(self::LOGIN_PREFIX);

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
        yield 'grant without on to user' => [
            self::TEST_GRANTS_WITHOUT_ON,
            null,
            [],
            false,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s]',
            // phpcs:ignore
            'REVOKE ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES FROM [%s]',
        ];

        yield 'grant without on to user with grant option' => [
            self::TEST_GRANTS_WITHOUT_ON,
            null,
            [],
            false,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];

        yield 'grant without on to role' => [
            self::TEST_GRANTS_WITHOUT_ON,
            null,
            [],
            false,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s_ROLE]',
            // phpcs:ignore
            'REVOKE ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES FROM [%s_ROLE]',
        ];

        yield 'grant without on to role with grant option' => [
            self::TEST_GRANTS_WITHOUT_ON,
            null,
            [],
            false,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT ALTER ANY EXTERNAL DATA SOURCE, ALTER ANY EXTERNAL FILE FORMAT, ADMINISTER DATABASE BULK OPERATIONS, CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, ALTER ANY DATASPACE, ALTER ANY ROLE, ALTER ANY SCHEMA, ALTER ANY USER, BACKUP DATABASE, CREATE PROCEDURE, CREATE ROLE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW, SHOWPLAN, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES TO [%s_ROLE] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];

        yield 'grant on schema to user' => [
            self::TEST_GRANTS_ON_SCHEMA,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s]',
            // phpcs:ignore
            'REVOKE CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] FROM [%s]',
        ];

        yield 'grant on schema to user with grant option' => [
            self::TEST_GRANTS_ON_SCHEMA,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];

        yield 'grant on schema to role' => [
            self::TEST_GRANTS_ON_SCHEMA,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s_ROLE]',
            // phpcs:ignore
            'REVOKE CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] FROM [%s_ROLE]',
        ];

        yield 'grant on schema to role with grant option' => [
            self::TEST_GRANTS_ON_SCHEMA,
            GrantOn::ON_SCHEMA,
            [self::TEST_SCHEMA],
            false,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, DELETE, EXECUTE, INSERT, SELECT, UPDATE, REFERENCES ON SCHEMA::[UTILS_TEST_GRANT_SCHEMA] TO [%s_ROLE] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];

        yield 'grant on user to user' => [
            self::TEST_GRANTS_ON_USER,
            GrantOn::ON_USER,
            [],
            true,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s]',
            // phpcs:ignore
            'REVOKE CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] FROM [%s]',
        ];

        yield 'grant on user to user with grant option' => [
            self::TEST_GRANTS_ON_USER,
            GrantOn::ON_USER,
            [],
            true,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];

        yield 'grant on user to role' => [
            self::TEST_GRANTS_ON_USER,
            GrantOn::ON_USER,
            [],
            true,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s_ROLE]',
            // phpcs:ignore
            'REVOKE CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] FROM [%s_ROLE]',
        ];

        yield 'grant on user to role with grant option' => [
            self::TEST_GRANTS_ON_USER,
            GrantOn::ON_USER,
            [],
            true,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION ON USER::[%s] TO [%s_ROLE] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];

        yield 'grant on table' => [
            self::TEST_GRANTS_ON_TABLE,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s]',
            // phpcs:ignore
            'REVOKE CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] FROM [%s]',
        ];
        yield 'grant on table to role' => [
            self::TEST_GRANTS_ON_TABLE,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            GrantOptions::OPTION_DONT_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s_ROLE]',
            // phpcs:ignore
            'REVOKE CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] FROM [%s_ROLE]',
        ];
        yield 'grant on table with grant option' => [
            self::TEST_GRANTS_ON_TABLE,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            false,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];
        yield 'grant on table to role with grant option' => [
            self::TEST_GRANTS_ON_TABLE,
            GrantOn::ON_OBJECT,
            [
                self::TEST_SCHEMA,
                self::TEST_TABLE,
            ],
            false,
            GrantOptions::OPTION_ALLOW_GRANT_OPTION,
            true,
            // phpcs:ignore
            'GRANT CONTROL, ALTER, VIEW DEFINITION, TAKE OWNERSHIP, INSERT, SELECT, UPDATE, REFERENCES ON OBJECT::[UTILS_TEST_GRANT_SCHEMA].[UTILS_TEST_GRANT_TABLE] TO [%s_ROLE] WITH GRANT OPTION',
            // phpcs:ignore
            'REVOKE ',
        ];
    }

    /**
     * @param array<Permission::GRANT_*> $permissions
     * @param null|GrantOn::ON_* $grantSubject
     * @param string[] $grantOnTargetPath
     * @throws \Doctrine\DBAL\Exception
     * @dataProvider grantDataProvider
     */
    public function testGrant(
        array $permissions,
        ?string $grantSubject,
        array $grantOnTargetPath,
        bool $useCurrentLoginOnEndPath,
        bool $allowGrantOption,
        bool $grantToRole,
        string $expectedGrant,
        string $expectedRevoke
    ): void {
        if ($useCurrentLoginOnEndPath === true) {
            $grantOnTargetPath[] = $this->currentLogin;
        }

        $grantTo = $this->currentLogin . ($grantToRole === true ? '_ROLE' : '');

        $options = (new GrantOptions(
            $permissions,
            $grantTo
        ))
            ->setOnTargetPath($grantOnTargetPath)
            ->grantOnSubject($grantSubject)
            ->setAllowGrantOption($allowGrantOption);

        $qb = new SynapseGrantQueryBuilder();
        $sql = $qb->getGrantSql($options);
        $this->connection->exec($sql);
        $this->assertSame(sprintf($expectedGrant, $this->currentLogin, $this->currentLogin), $sql);

        if ($allowGrantOption === true) {
            $this->markTestIncomplete('Revoking grant options doesn\'t work on Synapse.');
        }

        $options = (new RevokeOptions(
            $permissions,
            $grantTo
        ))
            ->setOnTargetPath($grantOnTargetPath)
            ->revokeOnSubject($grantSubject)
            ->revokeGrantOption($allowGrantOption)
            ->revokeInCascade(true);

        // revoke with cascade
        $sql = $qb->getRevokeSql($options);
        $this->connection->exec($sql);
        $this->assertSame(sprintf($expectedRevoke . ' CASCADE', $this->currentLogin, $this->currentLogin), $sql);

        // revoke without cascade
        $options->revokeInCascade(false);
        $sql = $qb->getRevokeSql($options);
        $this->connection->exec($sql);
        $this->assertSame(sprintf($expectedRevoke, $this->currentLogin, $this->currentLogin), $sql);
    }
}
