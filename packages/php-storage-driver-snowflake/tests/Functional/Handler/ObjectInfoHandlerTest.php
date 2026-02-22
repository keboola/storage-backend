<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Info\ObjectInfo;
use Keboola\StorageDriver\Command\Info\ObjectInfoCommand;
use Keboola\StorageDriver\Command\Info\ObjectInfoResponse;
use Keboola\StorageDriver\Command\Info\ObjectType;
use Keboola\StorageDriver\Command\Info\TableInfo;
use Keboola\StorageDriver\Command\Info\TableType as DriverTableType;
use Keboola\StorageDriver\Contract\Driver\Exception\ExceptionInterface;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\DatabaseMismatchException;
use Keboola\StorageDriver\Shared\Driver\Exception\Command\ObjectNotFoundException;
use Keboola\StorageDriver\Snowflake\Handler\Info\ObjectInfoHandler;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseProjectTestCase;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;

final class ObjectInfoHandlerTest extends BaseProjectTestCase
{
    protected const SCHEMA_NAME = 'object_info_schema';
    private const TABLE_NAME = 'object_info_table';
    private const VIEW_NAME = 'object_info_view';

    private Connection $projectConnection;

    public function testDatabaseInfo(): void
    {
        $this->projectConnection->executeStatement('CREATE SCHEMA "another_schema"');

        $command = new ObjectInfoCommand([
            'path' => [$this->projectResponse->getProjectDatabaseName()],
            'expectedObjectType' => ObjectType::DATABASE,
        ]);

        $response = (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::DATABASE, $response->getObjectType());

        $databaseInfo = $response->getDatabaseInfo();

        $this->assertNotNull($databaseInfo);
        $this->assertNull($response->getSchemaInfo());
        $this->assertNull($response->getTableInfo());
        $this->assertNull($response->getViewInfo());

        /** @var ObjectInfo[] $objects */
        $objects = $databaseInfo->getObjects();

        $map = [];
        foreach ($objects as $object) {
            $map[$object->getObjectName()] = $object->getObjectType();
        }

        ksort($map);
        $this->assertSame(
            [
               'another_schema' => 1,
               'object_info_schema' => 1,
            ],
            $map,
        );
    }

    public function testDatabaseMismatch(): void
    {
        $this->expectException(DatabaseMismatchException::class);
        $this->expectExceptionCode(ExceptionInterface::ERR_DATABASE_MISMATCH);
        $this->expectExceptionMessage(sprintf(
            'Requested database "nonexistent_database_xyz" does not match the connected database "%s".',
            $this->projectResponse->getProjectDatabaseName(),
        ));

        $command = new ObjectInfoCommand([
            'path' => ['nonexistent_database_xyz'],
            'expectedObjectType' => ObjectType::DATABASE,
        ]);

        (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testSchemaInfo(): void
    {
        $this->createTestTable();
        $this->projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s."another_table" (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));
        $this->createTestView();

        $command = new ObjectInfoCommand([
            'path' => [self::SCHEMA_NAME],
            'expectedObjectType' => ObjectType::SCHEMA,
        ]);

        $response = (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::SCHEMA, $response->getObjectType());

        $schemaInfo = $response->getSchemaInfo();

        $this->assertNotNull($schemaInfo);
        $this->assertNull($response->getDatabaseInfo());
        $this->assertNull($response->getTableInfo());
        $this->assertNull($response->getViewInfo());

        $objects = [];
        /** @var ObjectInfo $object */
        foreach ($schemaInfo->getObjects() as $object) {
            $objects[$object->getObjectName()] = $object->getObjectType();
        }

        ksort($objects);
        $this->assertSame(
            [
                'another_table' => 2,
                'object_info_table' => 2,
                'object_info_view' => 3,
            ],
            $objects,
        );
    }

    public function testSchemaInfoNotFound(): void
    {
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionCode(ExceptionInterface::ERR_SCHEMA_NOT_FOUND);
        $this->expectExceptionMessage('Object "nonexistent_schema_xyz" not found.');

        $command = new ObjectInfoCommand([
            'path' => ['nonexistent_schema_xyz'],
            'expectedObjectType' => ObjectType::SCHEMA,
        ]);

        (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testTableInfo(): void
    {
        $this->createTestTable();

        $command = new ObjectInfoCommand([
            'path' => [self::SCHEMA_NAME, self::TABLE_NAME],
            'expectedObjectType' => ObjectType::TABLE,
        ]);

        $response = (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::TABLE, $response->getObjectType());

        $tableInfo = $response->getTableInfo();

        $this->assertNotNull($tableInfo);
        $this->assertNull($response->getDatabaseInfo());
        $this->assertNull($response->getSchemaInfo());
        $this->assertNull($response->getViewInfo());

        $this->assertSame(self::TABLE_NAME, $tableInfo->getTableName());
        $this->assertSame([self::SCHEMA_NAME], iterator_to_array($tableInfo->getPath()));
        $this->assertSame(['ID'], iterator_to_array($tableInfo->getPrimaryKeysNames()));
        $this->assertSame(3, $tableInfo->getRowsCount());
        $this->assertGreaterThan(1500, $tableInfo->getSizeBytes());
        $this->assertSame(DriverTableType::NORMAL, $tableInfo->getTableType());

        /** @var TableInfo\TableColumn[] $columns */
        $columns = $tableInfo->getColumns();

        $map = [];
        foreach ($columns as $col) {
            $map[$col->getName()] = [
                'type' => $col->getType(),
                'nullable' => $col->getNullable(),
            ];
        }

        ksort($map);
        $this->assertSame(
            [
                'AGE' => ['type' => 'NUMBER', 'nullable' => true],
                'ID' => ['type' => 'NUMBER', 'nullable' => false],
                'NAME' => ['type' => 'VARCHAR', 'nullable' => true],
            ],
            $map,
        );
    }

    public function testTableInfoNotFound(): void
    {
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionCode(ExceptionInterface::ERR_TABLE_NOT_FOUND);
        $this->expectExceptionMessage('Object "nonexistent_table" not found.');

        $command = new ObjectInfoCommand([
            'path' => [self::SCHEMA_NAME, 'nonexistent_table'],
            'expectedObjectType' => ObjectType::TABLE,
        ]);

        (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    public function testViewInfo(): void
    {
        $this->createTestTable();
        $this->createTestView();

        $command = new ObjectInfoCommand([
            'path' => [self::SCHEMA_NAME, self::VIEW_NAME],
            'expectedObjectType' => ObjectType::VIEW,
        ]);

        $response = (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(ObjectInfoResponse::class, $response);
        $this->assertSame(ObjectType::VIEW, $response->getObjectType());

        $viewInfo = $response->getViewInfo();

        $this->assertNotNull($viewInfo);
        $this->assertNull($response->getDatabaseInfo());
        $this->assertNull($response->getSchemaInfo());
        $this->assertNull($response->getTableInfo());

        $this->assertSame(self::VIEW_NAME, $viewInfo->getViewName());
        $this->assertSame([self::SCHEMA_NAME], iterator_to_array($viewInfo->getPath()));

        /** @var TableInfo\TableColumn[] $columns */
        $columns = $viewInfo->getColumns();

        $map = [];
        foreach ($columns as $col) {
            $map[$col->getName()] = [
                'type' => $col->getType(),
                'nullable' => $col->getNullable(),
            ];
        }

        ksort($map);
        $this->assertSame(
            [
                'ID' => ['type' => 'NUMBER', 'nullable' => false],
                'NAME' => ['type' => 'VARCHAR', 'nullable' => true],
            ],
            $map,
        );
    }

    public function testViewInfoNotFound(): void
    {
        $this->expectException(ObjectNotFoundException::class);
        $this->expectExceptionCode(ExceptionInterface::ERR_VIEW_NOT_FOUND);
        $this->expectExceptionMessage('Object "nonexistent_view" not found.');

        $command = new ObjectInfoCommand([
            'path' => [self::SCHEMA_NAME, 'nonexistent_view'],
            'expectedObjectType' => ObjectType::VIEW,
        ]);

        (new ObjectInfoHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );
    }

    protected function setUp(): void
    {
        parent::setUp();

        $this->projectConnection = $this->getCurrentProjectConnection();
        $this->projectConnection->executeStatement(sprintf(
            'CREATE SCHEMA %s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));
    }

    private function createTestTable(): void
    {
        $this->projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.%s (ID INT NOT NULL, NAME VARCHAR(100), AGE NUMBER(10,0), PRIMARY KEY (ID))',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));

        $this->projectConnection->executeStatement(sprintf(
            'INSERT INTO %s.%s (ID, NAME, AGE) VALUES (1, \'Alice\', 30), (2, \'Bob\', 25), (3, \'Charlie\', 35)',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));
    }

    private function createTestView(): void
    {
        $this->projectConnection->executeStatement(sprintf(
            'CREATE VIEW %s.%s AS SELECT ID, NAME FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::VIEW_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));
    }
}
