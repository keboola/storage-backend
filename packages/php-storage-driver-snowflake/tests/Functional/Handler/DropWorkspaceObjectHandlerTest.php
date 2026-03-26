<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Doctrine\DBAL\Connection;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand;
use Keboola\StorageDriver\Command\Workspace\DropWorkspaceObjectCommand\WorkspaceObjectType;
use Keboola\StorageDriver\Snowflake\Handler\Workspace\DropObject\DropWorkspaceObjectHandler;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseProjectTestCase;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Throwable;

final class DropWorkspaceObjectHandlerTest extends BaseProjectTestCase
{
    protected const SCHEMA_NAME = 'drop_workspace_object_schema';
    private const TABLE_NAME = 'drop_ws_table';
    private const VIEW_NAME = 'drop_ws_view';

    private Connection $projectConnection;

    protected function setUp(): void
    {
        parent::setUp();

        $this->projectConnection = $this->getCurrentProjectConnection();
        $this->projectConnection->executeStatement(sprintf(
            'CREATE SCHEMA IF NOT EXISTS %s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));
    }

    protected function tearDown(): void
    {
        $this->projectConnection->executeStatement(sprintf(
            'DROP SCHEMA IF EXISTS %s CASCADE',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
        ));

        parent::tearDown();
    }

    public function testDropTable(): void
    {
        $this->projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.%s (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));

        $command = new DropWorkspaceObjectCommand([
            'workspaceObjectName' => self::SCHEMA_NAME,
            'objectNameToDrop' => self::TABLE_NAME,
            'objectType' => WorkspaceObjectType::TABLE,
        ]);

        $result = (new DropWorkspaceObjectHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertNull($result);

        $tables = $this->projectConnection->fetchAllAssociative(sprintf(
            'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND TABLE_TYPE = \'BASE TABLE\'',
            SnowflakeQuote::quote(strtoupper(self::SCHEMA_NAME)),
            SnowflakeQuote::quote(strtoupper(self::TABLE_NAME)),
        ));

        $this->assertCount(0, $tables, 'Table should have been dropped');
    }

    public function testDropView(): void
    {
        $this->projectConnection->executeStatement(sprintf(
            'CREATE TABLE %s.%s (ID INT)',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));
        $this->projectConnection->executeStatement(sprintf(
            'CREATE VIEW %s.%s AS SELECT ID FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::VIEW_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));

        $command = new DropWorkspaceObjectCommand([
            'workspaceObjectName' => self::SCHEMA_NAME,
            'objectNameToDrop' => self::VIEW_NAME,
            'objectType' => WorkspaceObjectType::VIEW,
        ]);

        $result = (new DropWorkspaceObjectHandler())(
            $this->getCurrentProjectCredentials(),
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertNull($result);

        $views = $this->projectConnection->fetchAllAssociative(sprintf(
            'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s',
            SnowflakeQuote::quote(strtoupper(self::SCHEMA_NAME)),
            SnowflakeQuote::quote(strtoupper(self::VIEW_NAME)),
        ));

        $this->assertCount(0, $views, 'View should have been dropped');
    }

    public function testDropNonExistentTable(): void
    {
        $command = new DropWorkspaceObjectCommand([
            'workspaceObjectName' => self::SCHEMA_NAME,
            'objectNameToDrop' => 'nonexistent_table_xyz',
            'objectType' => WorkspaceObjectType::TABLE,
        ]);

        $threw = false;
        try {
            (new DropWorkspaceObjectHandler())(
                $this->getCurrentProjectCredentials(),
                $command,
                [],
                new RuntimeOptions(),
            );
        } catch (Throwable) {
            $threw = true;
        }

        $this->assertTrue($threw, 'Handler should throw when dropping a non-existent table');
    }

    public function testDropNonExistentView(): void
    {
        $command = new DropWorkspaceObjectCommand([
            'workspaceObjectName' => self::SCHEMA_NAME,
            'objectNameToDrop' => 'nonexistent_view_xyz',
            'objectType' => WorkspaceObjectType::VIEW,
        ]);

        $threw = false;
        try {
            (new DropWorkspaceObjectHandler())(
                $this->getCurrentProjectCredentials(),
                $command,
                [],
                new RuntimeOptions(),
            );
        } catch (Throwable) {
            $threw = true;
        }

        $this->assertTrue($threw, 'Handler should throw when dropping a non-existent view');
    }
}
