<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Workspace;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceCommand;
use Keboola\StorageDriver\Command\Workspace\CreateWorkspaceResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Utils\Password;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Throwable;

final class CreateWorkspaceHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateWorkspaceCommand $command
     * @return CreateWorkspaceResponse|null
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateWorkspaceCommand);

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $nameGenerator = new NameGenerator($command->getStackPrefix());

        $workspaceSchemaName = sprintf('WS_%s', $command->getWorkspaceId());
        $workspaceRoleName = sprintf('WS_%s_ROLE', $command->getWorkspaceId());
        $workspaceUserName = sprintf('WS_%s_USER', $command->getWorkspaceId());
        $password = Password::generate();

        $database = $credentials->getPrincipal();

        try {
            $connection->executeQuery(sprintf(
                'CREATE ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
            ));

            $connection->executeQuery(sprintf(
                'GRANT USAGE ON DATABASE %s TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($database),
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
            ));

            $connection->executeQuery(sprintf(
                'CREATE TRANSIENT SCHEMA %s.%s',
                SnowflakeQuote::quoteSingleIdentifier($database),
                SnowflakeQuote::quoteSingleIdentifier($workspaceSchemaName),
            ));

            $connection->executeQuery(sprintf(
                'GRANT ALL PRIVILEGES ON SCHEMA %s.%s TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($database),
                SnowflakeQuote::quoteSingleIdentifier($workspaceSchemaName),
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
            ));

            $grants = $connection->fetchAllAssociative(sprintf(
                'SHOW GRANTS TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
            ));
            $streamlitGrant = array_filter($grants, fn($grant) => $grant['privilege'] === 'CREATE STREAMLIT');

            if (count($streamlitGrant) !== 0) {
                $connection->executeQuery(sprintf(
                    'REVOKE CREATE STREAMLIT ON SCHEMA %s.%s FROM ROLE %s',
                    SnowflakeQuote::quoteSingleIdentifier($database),
                    SnowflakeQuote::quoteSingleIdentifier($workspaceSchemaName),
                    SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                ));
            }

            foreach ($command->getSchemasForCreateTableGrants() as $schemaName) {
                assert(is_string($schemaName));
                $connection->executeQuery(sprintf(
                    'GRANT CREATE TABLE ON SCHEMA %s.%s TO ROLE %s',
                    SnowflakeQuote::quoteSingleIdentifier($database),
                    SnowflakeQuote::quoteSingleIdentifier($schemaName),
                    SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                ));
            }

            foreach ($command->getTablesForSelectInsertUpdateGrants() as $tableName) {
                assert(is_string($tableName));
                $parts = explode('.', $tableName);
                if (count($parts) === 2) {
                    $schemaName = $parts[0];
                    $tableNameOnly = $parts[1];

                    $connection->executeQuery(sprintf(
                        'GRANT SELECT, INSERT, UPDATE ON TABLE %s.%s.%s TO ROLE %s',
                        SnowflakeQuote::quoteSingleIdentifier($database),
                        SnowflakeQuote::quoteSingleIdentifier($schemaName),
                        SnowflakeQuote::quoteSingleIdentifier($tableNameOnly),
                        SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                    ));
                }
            }

            $connection->executeQuery(sprintf(
                'CREATE USER %s PASSWORD = %s DEFAULT_ROLE = %s',
                SnowflakeQuote::quoteSingleIdentifier($workspaceUserName),
                SnowflakeQuote::quote($password),
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
            ));

            $connection->executeQuery(sprintf(
                'GRANT ROLE %s TO USER %s',
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                SnowflakeQuote::quoteSingleIdentifier($workspaceUserName),
            ));

            $connection->executeQuery(sprintf(
                'GRANT ROLE %s TO ROLE %s',
                SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                SnowflakeQuote::quoteSingleIdentifier($command->getProjectRoleName()),
            ));

            if ($command->getProjectReadOnlyRoleName() !== '') {
                $connection->executeQuery(sprintf(
                    'GRANT ROLE %s TO ROLE %s',
                    SnowflakeQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName()),
                    SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                ));
            }

            if ($command->getDevBranchReadOnlyRoleName() !== '') {
                $connection->executeQuery(sprintf(
                    'GRANT ROLE %s TO ROLE %s',
                    SnowflakeQuote::quoteSingleIdentifier($command->getDevBranchReadOnlyRoleName()),
                    SnowflakeQuote::quoteSingleIdentifier($workspaceRoleName),
                ));
            }
        } catch (Throwable $e) {
            throw new WorkspaceCreationFailedException($e->getMessage(), $e->getCode(), $e);
        }

        return (new CreateWorkspaceResponse())
            ->setWorkspaceUserName($workspaceUserName)
            ->setWorkspaceRoleName($workspaceRoleName)
            ->setWorkspacePassword($password)
            ->setWorkspaceObjectName($workspaceSchemaName);
    }
}
