<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Project;

use Doctrine\DBAL\Connection;
use Google\ApiCore\ValidationException;
use Google\Protobuf\Any;
use Google\Protobuf\Internal\Message;
use InvalidArgumentException;
use Keboola\KeyGenerator\PemKeyCertificateGenerator;
use Keboola\StorageDriver\Command\Project\CreateProjectCommand;
use Keboola\StorageDriver\Command\Project\CreateProjectResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

/**
 * Do not use this handler as it is not fully tested yet.
 * It incorporates code from SnowflakeAssign class in Connection
 */
final class CreateProjectHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateProjectCommand $command
     * @return CreateProjectResponse
     * @throws ValidationException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProjectCommand);

        $commandMeta = $command->getMeta();
        if ($commandMeta !== null) {
            // override root user and use other database as root
            $commandMeta = $commandMeta->unpack();
            if (!$commandMeta instanceof CreateProjectCommand\CreateProjectSnowflakeMeta) {
                throw new InvalidArgumentException(sprintf(
                    'Expected CreateProjectCommand\CreateProjectSnowflakeMeta, got "%s"',
                    get_debug_type($commandMeta),
                ));
            }
            if ($command->getFileStorage() === CreateProjectCommand\FileStorageType::GCS
                && $commandMeta->getStorageIntegrationName() === ''
            ) {
                throw new InvalidArgumentException('Storage integration name is required for GCS file storage.');
            }
        } else {
            throw new Exception('CreateProjectBigqueryMeta is required.');
        }

        $nameGenerator = new NameGenerator($command->getStackPrefix());
        // create database
        $userName = $roleName = $dbName = $nameGenerator->createUserNameForProject($command->getProjectId());

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $connection->executeQuery(sprintf(
            'CREATE DATABASE %s DATA_RETENTION_TIME_IN_DAYS = %d',
            SnowflakeQuote::quoteSingleIdentifier($dbName),
            $command->getDataRetentionTime(),
        ));

        //create credentials
        $connection->executeQuery(sprintf(
            'CREATE ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        ));
        $connection->executeQuery(sprintf(
            'GRANT OWNERSHIP ON DATABASE %s TO ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($dbName),
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        ));

        // required for sql aliases
        $connection->executeQuery(sprintf(
            'GRANT CREATE ROLE ON ACCOUNT TO ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        ));

        // required for sql aliases
        $connection->executeQuery(sprintf(
            'GRANT CREATE USER ON ACCOUNT TO ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        ));

        if ($command->getFileStorage() === CreateProjectCommand\FileStorageType::GCS) {
            $connection->executeQuery(sprintf(
                'GRANT USAGE ON INTEGRATION %s TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($commandMeta->getStorageIntegrationName()),
                SnowflakeQuote::quoteSingleIdentifier($roleName),
            ));
        }

        $connection->executeQuery(sprintf(
            'CREATE USER %s DEFAULT_ROLE = %s TYPE=%s',
            SnowflakeQuote::quoteSingleIdentifier($userName),
            SnowflakeQuote::quoteSingleIdentifier($roleName),
            SnowflakeQuote::quote($commandMeta->getProjectUserLoginType()),
        ));

        $isNetworkPolicySet = $this->setNetworkPolicyForProjectUser(
            $connection,
            $roleName,
            $userName,
            $nameGenerator,
        );

        $connection->executeQuery(sprintf(
            'GRANT ROLE %s TO USER %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
            SnowflakeQuote::quoteSingleIdentifier($userName),
        ));

        $connection->executeQuery(sprintf(
            'ALTER USER %s SET RSA_PUBLIC_KEY=\'%s\'',
            SnowflakeQuote::quoteSingleIdentifier($userName),
            $commandMeta->getProjectUserPublicKey(),
        ));

        // it's intentionally below other queries, so that for dynamic backends service can use credentials from project
        // as does EnableWorkspaceSnowflakeDynamicBackendSize::execute, keeping them as close as possible
        if ($commandMeta->getSetupDynamicBackends()
            && !Features::isFeatureInList($features, Features::FEATURE_PAY_AS_YOU_GO)
        ) {
            foreach ($commandMeta->getAdditionalWarehousesToGrant() as $warehouseName) {
                assert(is_string($warehouseName));
                $connection->executeQuery(sprintf(
                    'GRANT USAGE ON WAREHOUSE %s TO ROLE %s WITH GRANT OPTION',
                    SnowflakeQuote::quoteSingleIdentifier($warehouseName),
                    SnowflakeQuote::quoteSingleIdentifier($roleName),
                ));
            }
        } else {
            // grant option is required for sql aliases
            $connection->executeQuery(sprintf(
                'GRANT USAGE ON WAREHOUSE %s TO ROLE %s WITH GRANT OPTION',
                SnowflakeQuote::quoteSingleIdentifier($commandMeta->getDefaultWarehouseToGrant()),
                SnowflakeQuote::quoteSingleIdentifier($roleName),
            ));
        }

        if (Features::isFeatureInList($features, Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE)) {
            // set secondary RSA key to be able to use this user to invoke commands
            $keyPair = (new PemKeyCertificateGenerator())->createPemKeyCertificate(null);
            $connection->executeQuery(sprintf(
                'ALTER USER %s SET RSA_PUBLIC_KEY_2=\'%s\'',
                SnowflakeQuote::quoteSingleIdentifier($userName),
                $keyPair->publicKey,
            ));
            $credentialsMeta = $credentials->getMeta();
            assert($credentialsMeta !== null);
            $connectionAsProjectUser = ConnectionFactory::createFromCredentials(
                (new GenericBackendCredentials())
                    ->setHost($credentials->getHost())
                    ->setPort($credentials->getPort())
                    ->setPrincipal($userName)
                    ->setSecret($keyPair->privateKey)
                    ->setMeta($credentialsMeta),
            );

            $readOnlyRoleName = $nameGenerator->createReadOnlyRoleNameForProject($command->getProjectId());
            $connectionAsProjectUser->executeQuery(sprintf(
                'CREATE OR REPLACE ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($readOnlyRoleName),
            ));
            $connectionAsProjectUser->executeQuery(sprintf(
                'GRANT USAGE ON DATABASE %s TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($dbName),
                SnowflakeQuote::quoteSingleIdentifier($readOnlyRoleName),
            ));
            $connectionAsProjectUser->executeQuery(sprintf(
                'GRANT ROLE %s TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($readOnlyRoleName),
                SnowflakeQuote::quoteSingleIdentifier($roleName),
            ));
        }
        $meta = new Any();
        $meta->pack((new CreateProjectResponse\CreateProjectSnowflakeMeta())
            ->setIsNetworkPolicySet($isNetworkPolicySet));

        return new CreateProjectResponse([
            'projectUserName' => $userName,
            'projectRoleName' => $roleName,
            'projectReadOnlyRoleName' => $readOnlyRoleName ?? '',
            'projectDatabaseName' => $dbName,
            'meta' => $meta,
        ]);
    }

    public function setNetworkPolicyForProjectUser(
        Connection $connection,
        string $roleName,
        string $userName,
        NameGenerator $nameGenerator,
    ): bool {
        $parameters = $connection->fetchAllAssociative(sprintf(
            'SHOW PARAMETERS LIKE \'network_policy\' IN USER %s',
            SnowflakeQuote::quoteSingleIdentifier($userName),
        ));
        if ($parameters[0]['value'] !== $nameGenerator->defaultNetworkPolicyName()) {
            // this backend does not have the policy set on the backend user, so we don't assign it to project user
            return false;
        }
        $defaultNetworkPolicyName = $nameGenerator->defaultNetworkPolicyName();
        $connection->executeQuery(sprintf(
            'ALTER USER %s SET NETWORK_POLICY = %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
            SnowflakeQuote::quote($defaultNetworkPolicyName),
        ));

        return true;
    }
}
