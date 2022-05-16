<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Teradata;

use Keboola\TableBackendUtils\Connection\ConnectionRetryWrapper;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataConnection;
use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;

class ConnectionTest extends TeradataBaseCase
{
    public const TEST_SCHEMA = self::TESTS_PREFIX . 'qb-schema';
    public const TEST_SCHEMA_2 = self::TESTS_PREFIX . 'qb-schema2';
    public const TEST_STAGING_TABLE = '#stagingTable';
    public const TEST_STAGING_TABLE_2 = '#stagingTable2';
    public const TEST_TABLE = self::TESTS_PREFIX . 'test';
    public const TEST_TABLE_2 = self::TESTS_PREFIX . 'test2';

    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanDatabase($this->getDatabaseName());
        $this->createDatabase($this->getDatabaseName());
    }

    public function testTeradataConnection(): void
    {
        $this->assertConnectionIsWorking($this->connection);
    }

    public function testGetDatabase(): void
    {
        $databaseName = $this->connection->executeQuery('SELECT DATABASE')->fetchOne();
        self::assertNotNull($databaseName);
    }

    public function testTeradataFetchAll(): void
    {
        $this->initTable();
        $data = [
            [1, 'franta', 'omacka'],
            [2, 'pepik', 'knedla'],
        ];
        foreach ($data as $item) {
            $this->insertRowToTable($this->getDatabaseName(), self::TABLE_GENERIC, ...$item);
        }
        $sqlSelect = sprintf(
            'SELECT * FROM %s.%s',
            TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
            TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        );
        $sqlSelectBindNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = :first_name',
            TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
            TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        );
        $sqlSelectBindMultipleNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = :first_name AND "last_name" = :last_name ',
            TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
            TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        );
        $sqlSelectBindNotNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = ?',
            TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
            TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        );
        $sqlSelectBindMultipleNotNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = ? AND "last_name" = ?',
            TeradataQuote::quoteSingleIdentifier($this->getDatabaseName()),
            TeradataQuote::quoteSingleIdentifier(self::TABLE_GENERIC)
        );

        $result = $this->connection->fetchAllAssociative($sqlSelect);
        $this->assertArrayEqualsSorted(
            [
                [
                    'id' => '1',
                    'first_name' => 'franta',
                    'last_name' => 'omacka',
                ],
                [
                    'id' => '2',
                    'first_name' => 'pepik',
                    'last_name' => 'knedla',
                ],
            ],
            $result,
            'id'
        );

        $result = $this->connection->fetchAllAssociative($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $this->connection->fetchAllAssociative($sqlSelectBindMultipleNamed, [
            'first_name' => 'franta',
            'last_name' => 'omacka',
        ]);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $this->connection->fetchAllAssociative($sqlSelectBindNotNamed, ['franta']);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $this->connection->fetchAllAssociative($sqlSelectBindMultipleNotNamed, ['franta', 'omacka']);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $this->connection->fetchAllAssociativeIndexed($sqlSelect);
        ksort($result);
        $this->assertSame([
            1 => [
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
            2 => [
                'first_name' => 'pepik',
                'last_name' => 'knedla',
            ],
        ], $result);

        $result = $this->connection->fetchAllAssociativeIndexed($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            1 => [
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $this->connection->fetchFirstColumn($sqlSelect);
        sort($result);
        $this->assertSame([
            '1',
            '2',
        ], $result);

        $result = $this->connection->fetchFirstColumn($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            '1',
        ], $result);

        $result = $this->connection->fetchAssociative($sqlSelectBindNamed, ['first_name' => 'pepik']);
        $this->assertSame([
            'id' => '2',
            'first_name' => 'pepik',
            'last_name' => 'knedla',

        ], $result);

        $result = $this->connection->fetchAllKeyValue($sqlSelect);
        ksort($result);
        $this->assertSame([
            1 => 'franta',
            2 => 'pepik',
        ], $result);

        $result = $this->connection->fetchAllKeyValue($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            1 => 'franta',
        ], $result);

        $result = $this->connection->fetchAllNumeric($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            [
                '1',
                'franta',
                'omacka',
            ],
        ], $result);
    }

    public function testWillDisconnect(): void
    {
        $wrappedConnection = TeradataConnection::getConnection([
            'host' => (string) getenv('TERADATA_HOST'),
            'user' => (string) getenv('TERADATA_USERNAME'),
            'password' => (string) getenv('TERADATA_PASSWORD'),
            'port' => (int) getenv('TERADATA_PORT'),
            'dbname' => '',
        ]);
        $wrappedConnection->connect(); // create odbc resource

        // get retry wrapper
        $wrappedConnectionRef = new \ReflectionClass($wrappedConnection);
        $wrappedConnectionPropRef = $wrappedConnectionRef->getProperty('_conn');
        $wrappedConnectionPropRef->setAccessible(true);
        /** @var ConnectionRetryWrapper $retryWrappedConnection */
        $retryWrappedConnection = $wrappedConnectionPropRef->getValue($wrappedConnection);
        // now get teradata connection from retry wrapper
        $retryWrappedConnectionRef = new \ReflectionClass($retryWrappedConnection);
        $retryWrappedConnectionPropRef = $retryWrappedConnectionRef->getProperty('connection');
        $retryWrappedConnectionPropRef->setAccessible(true);
        /** @var TeradataConnection $teradataConnection */
        $teradataConnection = $retryWrappedConnectionPropRef->getValue($retryWrappedConnection);
        // now get odbc connection
        $teradataConnectionRef = new \ReflectionClass($teradataConnection);
        $teradataConnectionPropRef = $teradataConnectionRef->getProperty('conn');
        $teradataConnectionPropRef->setAccessible(true);
        // check resource exists
        $this->assertIsResource($teradataConnectionPropRef->getValue($teradataConnection));
        $this->assertSame('odbc link', get_resource_type($teradataConnectionPropRef->getValue($teradataConnection)));
        $wrappedConnection->close();

        $this->assertNull($wrappedConnectionPropRef->getValue($wrappedConnection));
        // try reconnect
        $this->assertConnectionIsWorking($wrappedConnection);
        $wrappedConnection->close();
    }
}
