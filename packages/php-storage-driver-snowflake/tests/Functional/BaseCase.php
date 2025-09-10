<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Google\Protobuf\Any;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaQueryBuilder;
use PHPUnit\Framework\TestCase;

abstract class BaseCase extends TestCase
{
    protected const SCHEMA_NAME = 'profiling';

    protected Connection $connection;

    protected function getSnowflakeConnection(): Connection
    {
        $this->connection = ConnectionFactory::createFromCredentials($this->createCredentialsWithKeyPair());
        return $this->connection;
    }

    protected function createCredentialsWithPassword(): GenericBackendCredentials
    {
        $any = new Any();
        $any->pack(
            (new SnowflakeCredentialsMeta())
                ->setWarehouse((string) getenv('SNOWFLAKE_WAREHOUSE'))
                ->setDatabase((string) getenv('SNOWFLAKE_DATABASE')),
        );

        return (new GenericBackendCredentials())
            ->setHost((string) getenv('SNOWFLAKE_HOST'))
            ->setPort((int) getenv('SNOWFLAKE_PORT'))
            ->setPrincipal((string) getenv('SNOWFLAKE_USER'))
            ->setSecret((string) getenv('SNOWFLAKE_PASSWORD'))
            ->setMeta($any);
    }

    protected function createCredentialsWithKeyPair(): GenericBackendCredentials
    {
        $any = new Any();
        $any->pack(
            (new SnowflakeCredentialsMeta())
                ->setWarehouse((string) getenv('SNOWFLAKE_WAREHOUSE'))
                ->setDatabase((string) getenv('SNOWFLAKE_DATABASE')),
        );

        return (new GenericBackendCredentials())
            ->setHost((string) getenv('SNOWFLAKE_HOST'))
            ->setPort((int) getenv('SNOWFLAKE_PORT'))
            ->setPrincipal((string) getenv('SNOWFLAKE_USER'))
            ->setSecret((string) getenv('SNOWFLAKE_PRIVATE_KEY'))
            ->setMeta($any);
    }


    protected function setUp(): void
    {
        $this->getSnowflakeConnection();

        $this->connection->executeQuery(sprintf(
            'CREATE OR REPLACE SCHEMA %s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));
    }

    protected static function getRand(): string
    {
        return substr(md5(uniqid((string) mt_rand(), true)), 0, 3);
    }

    protected function getTestName(): string
    {
        return get_class($this) . '::' . $this->getName();
    }

    protected function getTestHash(): string
    {
        return md5($this->getTestName());
    }

    protected function getStackPrefix(): string
    {
        $stackPrefix = getenv('SNFLK_STACK_PREFIX');
        if ($stackPrefix === false) {
            $stackPrefix = 'local';
        }
        return $stackPrefix;
    }

    protected function tearDown(): void
    {
        $this->connection->executeQuery(
            (new SnowflakeSchemaQueryBuilder())->getDropSchemaCommand(self::SCHEMA_NAME),
        );
    }
}
