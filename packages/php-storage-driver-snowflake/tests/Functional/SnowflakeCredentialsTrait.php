<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Google\Protobuf\Any;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;

trait SnowflakeCredentialsTrait
{
    protected function normalizePrivateKey(string $privateKey): string
    {
        $privateKey = trim($privateKey);
        $privateKey = str_replace(["\r", "\n"], '', $privateKey);
        $privateKey = wordwrap($privateKey, 64, "\n", true);
        return "-----BEGIN PRIVATE KEY-----\n" . $privateKey . "\n-----END PRIVATE KEY-----\n";
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
            ->setSecret($this->normalizePrivateKey((string) getenv('SNOWFLAKE_PRIVATE_KEY')))
            ->setMeta($any);
    }
}
