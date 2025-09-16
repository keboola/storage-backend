<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\Handler\Bucket\CreateBucketHandler;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseProjectTestCase;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Throwable;

final class CreateBucketHandlerTest extends BaseProjectTestCase
{
    public function testCreateBucketInDefaultBranchWithReadOnlyStorageEnabled(): void
    {
        $command = new CreateBucketCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'DOES NOT MATTER',
            'bucketId' => 'in.c-test-bucket',
            'branchId' => 'DOES NOT MATTER',
            'projectRoleName' => 'DOES NOT MATTER',
            'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
            'devBranchReadOnlyRoleName' => 'DOES NOT MATTER',
            'isBranchDefault' => true,
        ]);
        $response = (new CreateBucketHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [
                Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            ],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);
        $this->assertSame('in.c-test-bucket', $response->getCreateBucketObjectName());
        $this->assertSame([], iterator_to_array($response->getPath()));

        // assert that ro role has access to the new schema
        // create new user and assign read only role
        // test that user can see the new schema
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectRoleName()),
            SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
        ));
        $testUserCredentials = $this->createTestUserWithCredentials(
            $this->getTestPrefix(),
            $this->projectResponse->getProjectDatabaseName(),
        );
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        $testUserConnection = ConnectionFactory::createFromCredentials($testUserCredentials);
        $this->assertVisibleSchemas([
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'in.c-test-bucket',
            ],
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'INFORMATION_SCHEMA',
            ],
        ], $testUserConnection);

        // assert that project user can create new table in schema
        $projectConnection = $this->getCurrentProjectConnection();
        $projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.test (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $projectConnection->executeStatement(sprintf(
            'INSERT INTO %s.test (ID) VALUES (1), (2), (3)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        // assert that user with ro role can read the data
        $values = $testUserConnection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.test',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $this->assertSame([
            ['ID' => '1'],
            ['ID' => '2'],
            ['ID' => '3'],
        ], $values);
    }

    public function testCreateBucketInDevBranchWithReadOnlyStorageEnabledAndRealStorageBranches(): void
    {
        $devBranchResponse = $this->createDevBranch();
        $command = new CreateBucketCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'DOES NOT MATTER',
            'bucketId' => 'in.c-test-bucket',
            'branchId' => '456',
            'projectRoleName' => 'DOES NOT MATTER',
            'projectReadOnlyRoleName' => 'DOES NOT MATTER',
            'devBranchReadOnlyRoleName' => $devBranchResponse->getDevBranchReadOnlyRoleName(),
            'isBranchDefault' => false,
        ]);
        $response = (new CreateBucketHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [
                Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
                Features::FEATURE_REAL_STORAGE_BRANCHES,
            ],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);
        $this->assertSame('456_in.c-test-bucket', $response->getCreateBucketObjectName());
        $this->assertSame([], iterator_to_array($response->getPath()));

        // assert that ro role has access to the new schema
        // grant project role to root user so we can grant role to test user
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectRoleName()),
            SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
        ));
        // create new user and assign read only role
        $testUserCredentials = $this->createTestUserWithCredentials(
            $this->getTestPrefix(),
            $this->projectResponse->getProjectDatabaseName(),
        );
        // assign project ro role and that that branch bucket is not visible
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        // test that user can see the new schema
        $testUserConnection = ConnectionFactory::createFromCredentials($testUserCredentials);
        $this->assertVisibleSchemas([
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'INFORMATION_SCHEMA',
            ],
        ], $testUserConnection);
        // revoke role
        $this->connection->executeQuery(sprintf(
            'REVOKE ROLE %s FROM USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        // assign project ro role and that that branch bucket is not visible
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($devBranchResponse->getDevBranchReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        // test that user can see the new schema
        $testUserConnection = ConnectionFactory::createFromCredentials($testUserCredentials);
        $this->assertVisibleSchemas([
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => '456_in.c-test-bucket',
            ],
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'INFORMATION_SCHEMA',
            ],
        ], $testUserConnection);
        // assert that project user can create new table in schema
        $projectConnection = $this->getCurrentProjectConnection();
        $projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.test (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $projectConnection->executeStatement(sprintf(
            'INSERT INTO %s.test (ID) VALUES (1), (2), (3)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        // assert that user with ro role can read the data
        $values = $testUserConnection->fetchAllAssociative(sprintf(
            'SELECT * FROM %s.test',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $this->assertSame([
            ['ID' => '1'],
            ['ID' => '2'],
            ['ID' => '3'],
        ], $values);
    }

    public function testCreateBucketInDevBranchWithReadOnlyStorageEnabledAndWithoutRealStorageBranches(): void
    {
        $command = new CreateBucketCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'DOES NOT MATTER',
            'bucketId' => 'in.c-test-bucket',
            'branchId' => '4321',
            'projectRoleName' => 'DOES NOT MATTER',
            'projectReadOnlyRoleName' => 'DOES NOT MATTER',
            'devBranchReadOnlyRoleName' => 'DOES NOT MATTER',
            'isBranchDefault' => false,
        ]);
        $response = (new CreateBucketHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [
                Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE,
            ],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);
        $this->assertSame('4321_in.c-test-bucket', $response->getCreateBucketObjectName());
        $this->assertSame([], iterator_to_array($response->getPath()));

        // assert that ro role has no access to the new schema
        // create new user and assign read only role
        // test that user can see the new schema
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectRoleName()),
            SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
        ));
        $testUserCredentials = $this->createTestUserWithCredentials(
            $this->getTestPrefix(),
            $this->projectResponse->getProjectDatabaseName(),
        );
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        $testUserConnection = ConnectionFactory::createFromCredentials($testUserCredentials);
        $this->assertVisibleSchemas([
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'INFORMATION_SCHEMA',
            ],
        ], $testUserConnection);

        // assert that project user can create new table in schema
        // this also tests that schema is created
        $projectConnection = $this->getCurrentProjectConnection();
        $projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.test (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $projectConnection->executeStatement(sprintf(
            'INSERT INTO %s.test (ID) VALUES (1), (2), (3)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        try {
            // assert that user with ro role cannot read the data
            $testUserConnection->fetchAllAssociative(sprintf(
                'SELECT * FROM %s.test',
                SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
            ));
            $this->fail('User with ro role should not have access to the new schema');
        } catch (Throwable $e) {
            $this->assertStringContainsString(sprintf(
                'Schema \'%s."%s"\' does not exist or not authorized.',
                $this->projectResponse->getProjectDatabaseName(),
                $response->getCreateBucketObjectName(),
            ), $e->getMessage());
        }
    }

    public function testCreateBucketInDefaultBranchWithoutReadOnlyStorageEnabled(): void
    {
        $command = new CreateBucketCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'DOES NOT MATTER',
            'bucketId' => 'in.c-test-bucket',
            'branchId' => 'DOES NOT MATTER',
            'projectRoleName' => 'DOES NOT MATTER',
            'projectReadOnlyRoleName' => $this->projectResponse->getProjectReadOnlyRoleName(),
            'devBranchReadOnlyRoleName' => 'DOES NOT MATTER',
            'isBranchDefault' => true,
        ]);
        $response = (new CreateBucketHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);
        $this->assertSame('in.c-test-bucket', $response->getCreateBucketObjectName());
        $this->assertSame([], iterator_to_array($response->getPath()));

        // assert that ro role has no access to the new schema
        // create new user and assign read only role
        // test that user can see the new schema
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectRoleName()),
            SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
        ));
        $testUserCredentials = $this->createTestUserWithCredentials(
            $this->getTestPrefix(),
            $this->projectResponse->getProjectDatabaseName(),
        );
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        $testUserConnection = ConnectionFactory::createFromCredentials($testUserCredentials);
        $this->assertVisibleSchemas([
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'INFORMATION_SCHEMA',
            ],
        ], $testUserConnection);

        // assert that project user can create new table in schema
        // this also tests that schema is created
        $projectConnection = $this->getCurrentProjectConnection();
        $projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.test (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $projectConnection->executeStatement(sprintf(
            'INSERT INTO %s.test (ID) VALUES (1), (2), (3)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        try {
            // assert that user with ro role cannot read the data
            $testUserConnection->fetchAllAssociative(sprintf(
                'SELECT * FROM %s.test',
                SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
            ));
            $this->fail('User with ro role should not have access to the new schema');
        } catch (Throwable $e) {
            $this->assertStringContainsString(sprintf(
                'Schema \'%s."%s"\' does not exist or not authorized.',
                $this->projectResponse->getProjectDatabaseName(),
                $response->getCreateBucketObjectName(),
            ), $e->getMessage());
        }
    }

    public function testCreateBucketInDevBranchWithoutReadOnlyStorageEnabled(): void
    {
        $command = new CreateBucketCommand([
            'stackPrefix' => $this->getTestPrefix(),
            'projectId' => 'DOES NOT MATTER',
            'bucketId' => 'in.c-test-bucket',
            'branchId' => '4321',
            'projectRoleName' => 'DOES NOT MATTER',
            'projectReadOnlyRoleName' => 'DOES NOT MATTER',
            'devBranchReadOnlyRoleName' => 'DOES NOT MATTER',
            'isBranchDefault' => false,
        ]);
        $response = (new CreateBucketHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
        $this->assertInstanceOf(CreateBucketResponse::class, $response);
        $this->assertSame('4321_in.c-test-bucket', $response->getCreateBucketObjectName());
        $this->assertSame([], iterator_to_array($response->getPath()));

        // assert that ro role has access to the new schema
        // grant project role to root user so we can grant role to test user
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectRoleName()),
            SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_USER')),
        ));
        // create new user and assign read only role
        $testUserCredentials = $this->createTestUserWithCredentials(
            $this->getTestPrefix(),
            $this->projectResponse->getProjectDatabaseName(),
        );
        $this->connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($this->projectResponse->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($testUserCredentials->getPrincipal()),
        ));
        // test that user can see the new schema
        $testUserConnection = ConnectionFactory::createFromCredentials($testUserCredentials);
        $this->assertVisibleSchemas([
            [
                'DATABASE_NAME' => $this->projectResponse->getProjectDatabaseName(),
                'SCHEMA_NAME' => 'INFORMATION_SCHEMA',
            ],
        ], $testUserConnection);
        // assert that project user can create new table in schema
        // this also tests that schema is created
        $projectConnection = $this->getCurrentProjectConnection();
        $projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.test (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        $projectConnection->executeStatement(sprintf(
            'INSERT INTO %s.test (ID) VALUES (1), (2), (3)',
            SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
        ));
        try {
            // assert that user with ro role cannot read the data
            $testUserConnection->fetchAllAssociative(sprintf(
                'SELECT * FROM %s.test',
                SnowflakeQuote::quoteSingleIdentifier($response->getCreateBucketObjectName()),
            ));
            $this->fail('User with ro role should not have access to the new schema');
        } catch (Throwable $e) {
            $this->assertStringContainsString(sprintf(
                'Schema \'%s."%s"\' does not exist or not authorized.',
                $this->projectResponse->getProjectDatabaseName(),
                $response->getCreateBucketObjectName(),
            ), $e->getMessage());
        }
    }

    /**
     * @param array<array{DATABASE_NAME:string,SCHEMA_NAME:string}> $expectedSchemas
     */
    private function assertVisibleSchemas(array $expectedSchemas, Connection $testUserConnection): void
    {
        $grants = $testUserConnection->fetchAllAssociative(<<<SQL
SELECT catalog_name AS database_name,
       schema_name
FROM information_schema.schemata;
SQL,);
        $this->assertSame(
            $expectedSchemas,
            $grants,
        );
    }
}
