<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Google\Protobuf\Any;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaQueryBuilder;
use PHPUnit\Framework\TestCase;

abstract class BaseCase extends TestCase
{
    use SnowflakeCredentialsTrait;

    protected const SCHEMA_NAME = 'profiling';

    protected Connection $connection;

    protected function setUp(): void
    {
        $this->getSnowflakeConnection();

        $this->connection->executeQuery(sprintf(
            'CREATE OR REPLACE SCHEMA %s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));
    }

    protected function getSnowflakeConnection(): Connection
    {
        $this->connection = ConnectionFactory::createFromCredentials($this->createCredentialsWithKeyPair());
        return $this->connection;
    }

    protected static function getRand(): string
    {
        return substr(md5(uniqid((string) mt_rand(), true)), 0, 3);
    }

    private function getTestName(): string
    {
        return get_class($this) . '::' . $this->getName();
    }

    private function getTestHash(): string
    {
        return md5($this->getTestName());
    }

    protected function getTestPrefix(): string
    {
        return $this->getStackPrefix() .'_'. $this->getTestHash();
    }

    private function getStackPrefix(): string
    {
        $stackPrefix = getenv('SNOWFLAKE_STACK_PREFIX');
        if (empty($stackPrefix)) {
            $stackPrefix = 'LOCAL_DRIVER_';
        }
        return $stackPrefix;
    }

    protected function tearDown(): void
    {
        $this->connection->executeQuery(
            (new SnowflakeSchemaQueryBuilder())->getDropSchemaCommand(self::SCHEMA_NAME),
        );
    }

    /**
     * @param array<int,Features::FEATURE_*> $features
     */
    protected function dropProjectForTest(string $stackPrefix, string $projectId, array $features = []): void
    {
        $nameGenerator = new NameGenerator($stackPrefix);
        $userName = $nameGenerator->createUserNameForProject($projectId);

        // this mimics the DropProjectHandler but all drops are optional
        $command = new DropProjectCommand([
            'projectUserName' => $userName,
            'projectRoleName' => $userName,
            'readOnlyRoleName' => $nameGenerator->createReadOnlyRoleNameForProject($projectId),
            'projectDatabaseName' => $userName,
        ]);

        $connection = $this->getSnowflakeConnection();
        $connection->executeQuery(sprintf(
            'DROP DATABASE IF EXISTS %s CASCADE',
            SnowflakeQuote::quoteSingleIdentifier(
                $command->getProjectDatabaseName(),
            ),
        ));

        $connection->executeQuery(sprintf(
            'DROP USER IF EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier(
                $command->getProjectUserName(),
            ),
        ));

        $connection->executeQuery(sprintf(
            'DROP ROLE IF EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier(
                $command->getProjectRoleName(),
            ),
        ));

        if (Features::isFeatureInList($features, Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE)) {
            // REVOKE is not needed, because the DB doesn't exists anymore
            $connection->executeQuery(sprintf(
                'DROP ROLE IF EXISTS %s',
                SnowflakeQuote::quoteSingleIdentifier($command->getReadOnlyRoleName()),
            ));
        }
    }

    protected function createTestUserWithCredentials(
        string $stackPrefix,
        string $databaseName,
    ): GenericBackendCredentials {
        $keyPair = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $testUserName = $stackPrefix . '_TEST_USER';
        $this->connection->executeStatement(sprintf(
            'CREATE OR REPLACE USER %s RSA_PUBLIC_KEY=\'%s\' DEFAULT_WAREHOUSE = %s TYPE = SERVICE',
            SnowflakeQuote::quoteSingleIdentifier($testUserName),
            $keyPair->publicKey,
            (string) getenv('SNOWFLAKE_WAREHOUSE'),
        ));
        $this->connection->executeStatement(sprintf(
            'GRANT USAGE ON WAREHOUSE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier((string) getenv('SNOWFLAKE_WAREHOUSE')),
            SnowflakeQuote::quoteSingleIdentifier($testUserName),
        ));
        $any = new Any();
        $any->pack(
            (new SnowflakeCredentialsMeta())
                ->setWarehouse((string) getenv('SNOWFLAKE_WAREHOUSE'))
                ->setDatabase($databaseName),
        );

        return (new GenericBackendCredentials())
            ->setHost((string) getenv('SNOWFLAKE_HOST'))
            ->setPort((int) getenv('SNOWFLAKE_PORT'))
            ->setPrincipal($testUserName)
            ->setSecret($keyPair->privateKey)
            ->setMeta($any);
    }
}
