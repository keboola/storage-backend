<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Project;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Project\DropDevBranchCommand;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class DropDevBranchHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param DropDevBranchCommand $command
     * @return null
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof DropDevBranchCommand);

        if (empty($command->getDevBranchReadOnlyRoleName())) {
            return null;
        }

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $sql = sprintf(
            'DROP ROLE IF EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier($command->getDevBranchReadOnlyRoleName()),
        );
        $connection->executeQuery($sql);

        return null;
    }
}
