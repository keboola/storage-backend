<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
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
