<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Workspace\DropObject;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;

final class DropWorkspaceObjectHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropWorkspaceObjectCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropWorkspaceObjectCommand);

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $schemaName = $command->getWorkspaceObjectName();
        $objectName = $command->getObjectNameToDrop();

        $reflection = new SnowflakeTableReflection($connection, $schemaName, $objectName);

        try {
            $isView = $reflection->isView();
        } catch (TableNotExistsReflectionException) {
            if ($command->getIgnoreIfNotExists()) {
                return null;
            }
            throw;
        }

        $quotedSchema = SnowflakeQuote::quoteSingleIdentifier($schemaName);
        $quotedName = SnowflakeQuote::quoteSingleIdentifier($objectName);

        $sql = $isView
            ? sprintf('DROP VIEW %s.%s', $quotedSchema, $quotedName)
            : sprintf('DROP TABLE %s.%s', $quotedSchema, $quotedName);

        $connection->executeQuery($sql);

        return null;
    }
}
