<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use Google\Protobuf\Any;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\KeyGenerator\PemKeyCertificatePair;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;
use Keboola\StorageDriver\Snowflake\Handler\Project\CreateProjectHandler;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

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

    /**
     * @param string $stackPrefix - it is advised to use $this->getTestPrefix() to avoid name collisions
     * @param string[] $features
     * @return array{0: CreateProjectResponse, 1: PemKeyCertificatePair}
     */
    protected function createProjectForTest(string $stackPrefix, string $projectId, array $features = []): array
    {
        $keyPair = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
        $meta = new Any();
        $meta->pack(new CreateProjectCommand\CreateProjectSnowflakeMeta([
            'projectUserLoginType' => 'SERVICE',
            'projectUserPublicKey' => $keyPair->publicKey,
            'setupDynamicBackends' => false,
            'defaultWarehouseToGrant' => 'DEV',
        ]));
        $response = (new CreateProjectHandler())->__invoke(
            $this->createCredentialsWithKeyPair(),
            new CreateProjectCommand([
                'stackPrefix' => $stackPrefix,
                'projectId' => $projectId,
                'dataRetentionTime' => 0,
                'fileStorage' => CreateProjectCommand\FileStorageType::S3,
                'meta' => $meta,
            ]),
            $features,
            new RuntimeOptions(),
        );
        assert($response instanceof CreateProjectResponse);
        return [$response, $keyPair];
    }

    protected function createProjectCredentials(
        CreateProjectResponse $response,
        PemKeyCertificatePair $certificatePair,
    ): GenericBackendCredentials {
        $any = new Any();
        $any->pack(
            (new SnowflakeCredentialsMeta())
                ->setWarehouse((string) getenv('SNOWFLAKE_WAREHOUSE'))
                ->setDatabase($response->getProjectDatabaseName()),
        );

        return (new GenericBackendCredentials())
            ->setHost((string) getenv('SNOWFLAKE_HOST'))
            ->setPort((int) getenv('SNOWFLAKE_PORT'))
            ->setPrincipal($response->getProjectUserName())
            ->setSecret($certificatePair->privateKey)
            ->setMeta($any);
    }
}
