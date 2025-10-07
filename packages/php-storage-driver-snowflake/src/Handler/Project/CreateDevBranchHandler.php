<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Project;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\CreateDevBranchCommand;
use Keboola\StorageDriver\Command\Project\CreateDevBranchResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class CreateDevBranchHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateDevBranchCommand $command
     * @return CreateDevBranchResponse|null
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateDevBranchCommand);

        if (!Features::isFeatureInList($features, Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE)) {
            return null;
        }

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $roleName = (new NameGenerator($command->getStackPrefix()))
            ->createReadOnlyRoleNameForBranch(
                $command->getProjectId(),
                $command->getBranchId(),
            );

        $sql = sprintf(
            'CREATE OR REPLACE ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        );
        $connection->executeQuery($sql);
        // Grant project read only role to the dev branch read only role
        // This will allow this role to have read access to all buckets in project
        $sql = sprintf(
            'GRANT ROLE %s TO ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
            SnowflakeQuote::quoteSingleIdentifier($roleName),
        );
        $connection->executeQuery($sql);
        // Grant branch read only role to the project role
        // This will allow project role to have access to all objects in dev branch role
        $sql = sprintf(
            'GRANT ROLE %s TO ROLE %s',
            SnowflakeQuote::quoteSingleIdentifier($roleName),
            SnowflakeQuote::quoteSingleIdentifier($command->getProjectRoleName()),
        );
        $connection->executeQuery($sql);

        return (new CreateDevBranchResponse())
            ->setDevBranchReadOnlyRoleName($roleName);
    }
}
