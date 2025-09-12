<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Project;

use Google\ApiCore\ValidationException;
use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\DropProjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use RuntimeException;

/**
 * Do not use this handler as it is not fully tested yet.
 * It incorporates code from SnowflakeAssign class in Connection
 */
final class DropProjectHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropProjectCommand $command
     * @return null
     * @throws ValidationException
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): ?Message {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropProjectCommand);

        $connection = ConnectionFactory::createFromCredentials(
            $credentials,
        );
        // grant project role to current role to be able to drop the project database
        // role is dropped later on with this grant
        $currentRole = $connection->fetchOne('SELECT CURRENT_ROLE()');
        if (!is_string($currentRole)) {
            throw new RuntimeException('Cannot get current role.');
        }
        $connection->executeQuery(sprintf(
            'GRANT ROLE %s TO ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($command->getProjectRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($currentRole),
        ));

        // assume project role to be able to drop the project database
        $connection->executeQuery(sprintf(
            'USE ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($command->getProjectRoleName()),
        ));

        $connection->executeQuery(sprintf(
            'DROP DATABASE %s CASCADE',
            SnowflakeQuote::quoteSingleIdentifier(
                $command->getProjectDatabaseName(),
            ),
        ));

        $connection->executeQuery(sprintf(
            'DROP USER %s',
            SnowflakeQuote::quoteSingleIdentifier(
                $command->getProjectUserName(),
            ),
        ));

        $connection->executeQuery(sprintf(
            'DROP ROLE %s',
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

        return null;
    }
}
