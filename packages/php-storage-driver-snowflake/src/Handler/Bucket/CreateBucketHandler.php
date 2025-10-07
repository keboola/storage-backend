<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Bucket;

use Google\Protobuf\Internal\Message;
use Keboola\StorageDriver\Command\Bucket\CreateBucketCommand;
use Keboola\StorageDriver\Command\Bucket\CreateBucketResponse;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Features;
use Keboola\StorageDriver\Snowflake\NameGenerator;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Throwable;

final class CreateBucketHandler extends BaseHandler
{

    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateBucketCommand $command
     * @return CreateBucketResponse|null
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateBucketCommand);

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $bucketSchemaName = (new NameGenerator($command->getStackPrefix()))
            ->createObjectNameForBucketInProject(
                $command->getBucketId(),
                $command->getIsBranchDefault(),
                $command->getBranchId(),
            );

        $quotedRoleToGrant = null;
        if (Features::isFeatureInList($features, Features::FEATURE_INPUT_MAPPING_READ_ONLY_STORAGE)) {
            if ($command->getIsBranchDefault()) {
                // in default branch we do grant to new schema and tables to the project read only role
                $quotedRoleToGrant = SnowflakeQuote::quoteSingleIdentifier($command->getProjectReadOnlyRoleName());
            } elseif (Features::isOneOfFeaturesInList($features, [
                Features::FEATURE_PROTECTED_DEFAULT_BRANCH,
                Features::FEATURE_REAL_STORAGE_BRANCHES,
            ])) {
                // in dev branch in case real storage in dev branches is enabled
                // we grant schema and tables to the dev branch read only role only
                // in default branch we do grant to new schema and tables to the project read only role
                $quotedRoleToGrant = SnowflakeQuote::quoteSingleIdentifier($command->getDevBranchReadOnlyRoleName());
            }
        }

        try {
            $quotedSchemaName = SnowflakeQuote::quoteSingleIdentifier($bucketSchemaName);

            $connection->executeQuery(sprintf(
                'CREATE SCHEMA %s WITH MANAGED ACCESS',
                $quotedSchemaName,
            ));

            if ($quotedRoleToGrant !== null) {
                $connection->executeQuery(sprintf(
                    'GRANT USAGE ON SCHEMA %s TO ROLE %s',
                    $quotedSchemaName,
                    $quotedRoleToGrant,
                ));

                $connection->executeQuery(sprintf(
                    'GRANT SELECT ON FUTURE TABLES IN SCHEMA %s TO ROLE %s',
                    $quotedSchemaName,
                    $quotedRoleToGrant,
                ));
            }
        } catch (Throwable $e) {
            throw new BucketCreationFailedException($e->getMessage(), $e->getCode(), $e);
        }

        return (new CreateBucketResponse())
            ->setCreateBucketObjectName($bucketSchemaName);
    }
}
