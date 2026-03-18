<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Info;

use Doctrine\DBAL\Connection;
use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\Message;
use Google\Protobuf\Internal\RepeatedField;
use Keboola\StorageDriver\Command\Info\DatabaseInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\SchemaInfo;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\DatabaseMismatchException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\UnknownObjectException;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Schema\Snowflake\SnowflakeSchemaReflection;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;
use Keboola\TableBackendUtils\TableNotExistsReflectionException;

final class ObjectInfoHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param ObjectInfoCommand $command
     * @return ObjectInfoResponse
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        /** @phpstan-ignore function.alreadyNarrowedType, instanceof.alwaysTrue */
        assert($credentials instanceof GenericBackendCredentials);
        /** @phpstan-ignore function.alreadyNarrowedType, instanceof.alwaysTrue */
        assert($command instanceof ObjectInfoCommand);

        $connection = ConnectionFactory::createFromCredentials($credentials);

        $path = ProtobufHelper::repeatedStringToArray($command->getPath());

        assert(count($path) !== 0, 'Error empty path.');

        $response = (new ObjectInfoResponse())
            ->setPath($command->getPath())
            ->setObjectType($command->getExpectedObjectType());

        switch ($command->getExpectedObjectType()) {
            case ObjectType::DATABASE:
                return $this->getDatabaseResponse($path, $connection, $response);
            case ObjectType::SCHEMA:
                return $this->getSchemaResponse($path, $connection, $response);
            case ObjectType::TABLE:
                return $this->getTableResponse($path, $connection, $response);
            case ObjectType::VIEW:
                return $this->getViewResponse($path, $connection, $response);
            default:
                $typeName = ObjectType::name($command->getExpectedObjectType());
                assert(is_string($typeName));
                throw new UnknownObjectException($typeName);
        }
    }

    /**
     * @param string[] $path
     */
    private function getDatabaseResponse(
        array $path,
        Connection $connection,
        ObjectInfoResponse $response,
    ): ObjectInfoResponse {
        assert(count($path) === 1, 'Error path must have exactly one element.');

        $dbName = $path[0];

        // Validate that requested database is the current connected database
        $currentDb = $connection->fetchOne('SELECT CURRENT_DATABASE()');
        assert(is_string($currentDb));
        if (strtoupper($currentDb) !== strtoupper($dbName)) {
            throw new DatabaseMismatchException($dbName, $currentDb);
        }

        /** @var array<array{SCHEMA_NAME: string}> $schemas */
        $schemas = $connection->fetchAllAssociative(
            'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME',
        );

        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        foreach ($schemas as $schema) {
            $name = $schema['SCHEMA_NAME'];

            if ($name === 'INFORMATION_SCHEMA') {
                continue;
            }

            $objects[] = (new ObjectInfo())
                ->setObjectType(ObjectType::SCHEMA)
                ->setObjectName($name);
        }

        $infoObject = new DatabaseInfo();
        $infoObject->setObjects($objects);
        $response->setDatabaseInfo($infoObject);

        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getSchemaResponse(
        array $path,
        Connection $connection,
        ObjectInfoResponse $response,
    ): ObjectInfoResponse {
        assert(count($path) === 1, 'Error path must have exactly one element.');

        $schemaName = $path[0];

        // Validate schema exists
        /** @var array<array{SCHEMA_NAME: string}> $schemas */
        $schemas = $connection->fetchAllAssociative(
            sprintf(
                'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = %s',
                SnowflakeQuote::quote($schemaName),
            ),
        );

        if (count($schemas) === 0) {
            throw new ObjectNotFoundException($schemaName, ExceptionInterface::ERR_SCHEMA_NOT_FOUND);
        }

        $schemaReflection = new SnowflakeSchemaReflection($connection, $schemaName);

        $objects = new RepeatedField(GPBType::MESSAGE, ObjectInfo::class);
        foreach ($schemaReflection->getTablesNames() as $tableName) {
            $objects[] = (new ObjectInfo())
                ->setObjectType(ObjectType::TABLE)
                ->setObjectName($tableName);
        }
        foreach ($schemaReflection->getViewsNames() as $viewName) {
            $objects[] = (new ObjectInfo())
                ->setObjectType(ObjectType::VIEW)
                ->setObjectName($viewName);
        }

        $infoObject = new SchemaInfo();
        $infoObject->setObjects($objects);
        $response->setSchemaInfo($infoObject);

        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getTableResponse(
        array $path,
        Connection $connection,
        ObjectInfoResponse $response,
    ): ObjectInfoResponse {
        assert(count($path) === 2, 'Error path must have exactly two elements.');

        try {
            $response->setTableInfo(
                TableReflectionResponseTransformer::transformTableReflectionToResponse(
                    $path[0],
                    new SnowflakeTableReflection(
                        $connection,
                        $path[0],
                        $path[1],
                    ),
                ),
            );
        } catch (TableNotExistsReflectionException) {
            throw new ObjectNotFoundException($path[1], ExceptionInterface::ERR_TABLE_NOT_FOUND);
        }

        return $response;
    }

    /**
     * @param string[] $path
     */
    private function getViewResponse(
        array $path,
        Connection $connection,
        ObjectInfoResponse $response,
    ): ObjectInfoResponse {
        assert(count($path) === 2, 'Error path must have exactly two elements.');

        try {
            $response->setViewInfo(
                ViewReflectionResponseTransformer::transformTableReflectionToResponse(
                    $path[0],
                    new SnowflakeTableReflection(
                        $connection,
                        $path[0],
                        $path[1],
                    ),
                ),
            );
        } catch (TableNotExistsReflectionException) {
            throw new ObjectNotFoundException($path[1], ExceptionInterface::ERR_VIEW_NOT_FOUND);
        }

        return $response;
    }
}
