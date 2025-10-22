<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Handler\Workspace\CreateWorkspaceHandler;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseProjectTestCase;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Throwable;

final class CreateWorkspaceHandlerTest extends BaseProjectTestCase
{
    protected function tearDown(): void
    {
        parent::tearDown();

        $connection = $this->getCurrentProjectConnection();

        $workspaces = $connection->fetchAllAssociative(sprintf(
            'SHOW SCHEMAS LIKE %s IN DATABASE %s',
            SnowflakeQuote::quote('%WORKSPACE_%'),
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
        ));

        foreach ($workspaces as $workspace) {
            assert(is_array($workspace));
            assert(isset($workspace['name']));
            assert(is_string($workspace['name']));
            $schemaName = $workspace['name'];
            $roleName = $this->getTestPrefix() . $schemaName;
            $userName = $this->getTestPrefix() . $schemaName;

            try {
                $connection->executeQuery(sprintf(
                    'DROP SCHEMA IF EXISTS %s.%s CASCADE',
                    SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
                    SnowflakeQuote::quoteSingleIdentifier($schemaName),
                ));
            } catch (Throwable $e) {
            }

            try {
                $connection->executeQuery(sprintf(
                    'DROP ROLE IF EXISTS %s',
                    SnowflakeQuote::quoteSingleIdentifier($roleName),
                ));
            } catch (Throwable $e) {
            }

            try {
                $connection->executeQuery(sprintf(
                    'DROP USER IF EXISTS %s',
                    SnowflakeQuote::quoteSingleIdentifier($userName),
                ));
            } catch (Throwable $e) {
            }
        }
    }

    public function testCreateWorkspaceBasic(): void
    {
        $workspaceId = '12345';
        $command = new CreateWorkspaceCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'TEST_PROJECT',
            'workspaceId' => $workspaceId,
            'branchId' => '1',
            'isBranchDefault' => true,
            'projectUserName' => $this->projectResponse->getProjectDatabaseName(),
            'projectRoleName' => $this->projectResponse->getProjectRoleName(),
            'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
            'devBranchReadOnlyRoleName' => '',
        ]);

        $response = (new CreateWorkspaceHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);
        $expectedSchemaName = sprintf('WORKSPACE_%s', $workspaceId);
        $expectedUserName = sprintf('%sWORKSPACE_%s', $this->getTestPrefix(), $workspaceId);
        $expectedRoleName = $expectedUserName;

        $this->assertSame($expectedUserName, $response->getWorkspaceUserName());
        $this->assertSame($expectedRoleName, $response->getWorkspaceRoleName());
        $this->assertSame($expectedSchemaName, $response->getWorkspaceObjectName());
        $this->assertNotEmpty($response->getWorkspacePassword());

        $connection = $this->getCurrentProjectConnection();
        $schemas = $connection->fetchAllAssociative(sprintf(
            'SHOW SCHEMAS LIKE %s IN DATABASE %s',
            SnowflakeQuote::quote($expectedSchemaName),
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
        ));
        $this->assertCount(1, $schemas);
        $this->assertSame($expectedSchemaName, $schemas[0]['name']);

        $roles = $connection->fetchAllAssociative(sprintf(
            'SHOW ROLES LIKE %s',
            SnowflakeQuote::quote($expectedRoleName),
        ));
        $this->assertCount(1, $roles);

        $users = $connection->fetchAllAssociative(sprintf(
            'SHOW USERS LIKE %s',
            SnowflakeQuote::quote($expectedUserName),
        ));
        $this->assertCount(1, $users);
    }

    public function testCreateWorkspaceWithDirectGrantPermissions(): void
    {
        $connection = $this->getCurrentProjectConnection();

        $testBucketSchema = 'out.c-test-bucket';
        $connection->executeQuery(sprintf(
            'CREATE SCHEMA IF NOT EXISTS %s.%s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));

        $connection->executeQuery(sprintf(
            'CREATE TABLE IF NOT EXISTS %s.%s.test_table (id INT, name VARCHAR(100))',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));

        $connection->executeQuery(sprintf(
            'INSERT INTO %s.%s.test_table (id, name) VALUES (1, %s), (2, %s)',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
            SnowflakeQuote::quote('Alice'),
            SnowflakeQuote::quote('Bob'),
        ));

        $workspaceId = '67890';
        $command = new CreateWorkspaceCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'TEST_PROJECT',
            'workspaceId' => $workspaceId,
            'branchId' => '1',
            'isBranchDefault' => true,
            'projectUserName' => $this->projectResponse->getProjectDatabaseName(),
            'projectRoleName' => $this->projectResponse->getProjectRoleName(),
            'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
            'devBranchReadOnlyRoleName' => '',
            'schemasForCreateTableGrants' => [$testBucketSchema],
            'tablesForSelectInsertUpdateGrants' => [sprintf('%s.test_table', $testBucketSchema)],
        ]);

        $response = (new CreateWorkspaceHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(CreateWorkspaceResponse::class, $response);

        $workspaceUserCredentials = $this->createCredentialsFromResponse($response);
        $workspaceConnection = ConnectionFactory::createFromCredentials($workspaceUserCredentials);

        $workspaceConnection->executeQuery(sprintf(
            'USE ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($response->getWorkspaceRoleName()),
        ));

        $data = $workspaceConnection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.%s.test_table ORDER BY id',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));

        $this->assertCount(2, $data);
        $this->assertSame('1', $data[0]['ID']);
        $this->assertSame('Alice', $data[0]['NAME']);
        $this->assertSame('2', $data[1]['ID']);
        $this->assertSame('Bob', $data[1]['NAME']);

        $workspaceConnection->executeQuery(sprintf(
            'INSERT INTO %s.%s.test_table (id, name) VALUES (3, %s)',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
            SnowflakeQuote::quote('Charlie'),
        ));

        $data = $workspaceConnection->fetchAllAssociative(sprintf(
            'SELECT COUNT(*) as cnt FROM %s.%s.test_table',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));
        $this->assertSame('3', $data[0]['CNT']);

        $workspaceConnection->executeQuery(sprintf(
            'UPDATE %s.%s.test_table SET name = %s WHERE id = 1',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
            SnowflakeQuote::quote('Alice Updated'),
        ));

        $data = $workspaceConnection->fetchAllAssociative(sprintf(
            'SELECT name FROM %s.%s.test_table WHERE id = 1',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));
        $this->assertSame('Alice Updated', $data[0]['NAME']);

        $workspaceConnection->executeQuery(sprintf(
            'CREATE TABLE %s.%s.new_table (id INT)',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));

        $tables = $workspaceConnection->fetchAllAssociative(sprintf(
            'SHOW TABLES LIKE %s IN SCHEMA %s.%s',
            SnowflakeQuote::quote('new_table'),
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));
        $this->assertCount(1, $tables);

        $connection->executeQuery(sprintf(
            'DROP TABLE IF EXISTS %s.%s.new_table',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));

        $connection->executeQuery(sprintf(
            'DROP TABLE IF EXISTS %s.%s.test_table',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));

        $connection->executeQuery(sprintf(
            'DROP SCHEMA IF EXISTS %s.%s CASCADE',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectDatabaseName()),
            SnowflakeQuote::quoteSingleIdentifier($testBucketSchema),
        ));
    }

    private function createCredentialsFromResponse(
        CreateWorkspaceResponse $response,
    ): GenericBackendCredentials {
        return (new GenericBackendCredentials())
            ->setHost((string) getenv('SNOWFLAKE_HOST'))
            ->setPort((int) getenv('SNOWFLAKE_PORT'))
            ->setPrincipal($response->getWorkspaceUserName())
            ->setSecret($response->getWorkspacePassword());
    }
}
