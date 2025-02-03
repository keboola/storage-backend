<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Functional\Snowflake;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\StringTooLongException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\WarehouseTimeoutReached;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnection;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeConnectionFactory;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use ReflectionClass;

class ConnectionTest extends SnowflakeBaseCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->cleanSchema(self::TEST_SCHEMA);
    }

    /**
     * @dataProvider connectionProvider
     */
    public function testSnowflakeConnection(Connection $connection): void
    {
        $this->assertConnectionIsWorking($connection);
    }

    /**
     * @dataProvider connectionProvider
     */
    public function testSnowflakeFetchAll(Connection $connection): void
    {
        $this->initTable();
        $data = [
            [1, 'franta', 'omacka'],
            [2, 'pepik', 'knedla'],
        ];
        foreach ($data as $item) {
            $this->insertRowToTable(self::TEST_SCHEMA, self::TABLE_GENERIC, ...$item);
        }
        $sqlSelect = sprintf(
            'SELECT * FROM %s.%s',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
        );
        $sqlSelectBindNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = :first_name',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
        );
        $sqlSelectBindMultipleNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = :first_name AND "last_name" = :last_name ',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
        );
        $sqlSelectBindNotNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = ?',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
        );
        $sqlSelectBindMultipleNotNamed = sprintf(
            'SELECT * FROM %s.%s WHERE "first_name" = ? AND "last_name" = ?',
            SnowflakeQuote::quoteSingleIdentifier(self::TEST_SCHEMA),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_GENERIC),
        );

        $result = $connection->fetchAllAssociative($sqlSelect);
        $this->assertSame([
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
        ], $result);

        $result = $connection->fetchAllAssociative($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $connection->fetchAllAssociative($sqlSelectBindMultipleNamed, [
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

        $result = $connection->fetchAllAssociative($sqlSelectBindNotNamed, ['franta']);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $connection->fetchAllAssociative($sqlSelectBindMultipleNotNamed, ['franta', 'omacka']);
        $this->assertSame([
            [
                'id' => '1',
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $connection->fetchAllAssociativeIndexed($sqlSelect);
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

        $result = $connection->fetchAllAssociativeIndexed($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            1 => [
                'first_name' => 'franta',
                'last_name' => 'omacka',
            ],
        ], $result);

        $result = $connection->fetchFirstColumn($sqlSelect);
        $this->assertSame([
            '1',
            '2',
        ], $result);

        $result = $connection->fetchFirstColumn($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            '1',
        ], $result);

        $result = $connection->fetchAssociative($sqlSelect);
        $this->assertSame([
            'id' => '1',
            'first_name' => 'franta',
            'last_name' => 'omacka',

        ], $result);

        $result = $connection->fetchAssociative($sqlSelectBindNamed, ['first_name' => 'pepik']);
        $this->assertSame([
            'id' => '2',
            'first_name' => 'pepik',
            'last_name' => 'knedla',

        ], $result);

        $result = $connection->fetchAllKeyValue($sqlSelect);
        $this->assertSame([
            1 => 'franta',
            2 => 'pepik',
        ], $result);

        $result = $connection->fetchAllKeyValue($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            1 => 'franta',
        ], $result);

        $result = $connection->fetchAllNumeric($sqlSelect);
        $this->assertSame([
            [
                '1',
                'franta',
                'omacka',
            ],
            [
                '2',
                'pepik',
                'knedla',
            ],
        ], $result);

        $result = $connection->fetchAllNumeric($sqlSelectBindNamed, ['first_name' => 'franta']);
        $this->assertSame([
            [
                '1',
                'franta',
                'omacka',
            ],
        ], $result);
    }

    public function testStringToLong(): void
    {
        $this->initTable();
        $longString= str_repeat('a', 101);
        $this->expectException(StringTooLongException::class);
        $this->expectExceptionMessage(sprintf(
            // phpcs:ignore
            'An exception occurred while executing a query: String \'%s\' cannot be inserted because it\'s bigger than column size',
            $longString,
        ));
        $this->insertRowToTable(
            self::TEST_SCHEMA,
            self::TABLE_GENERIC,
            1,
            'franta',
            $longString,
        );
    }

    public function testInvalidCredentials(): void
    {
        $connection = SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            'invalid',
            'invalid',
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
            ],
        );

        $this->expectException(DriverException::class);
        $this->expectExceptionMessage(
            'An exception occurred in the driver: Incorrect username or password was specified.',
        );
        $this->assertConnectionIsWorking($connection);
    }

    public function testInvalidAccessToDatabase(): void
    {
        $connection = SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => 'invalidDatabase',
            ],
        );

        $this->assertConnectionIsWorking($connection);
        $this->assertNull($connection->fetchOne('SELECT CURRENT_DATABASE()'));
        $this->connection->close();
        $this->assertConnectionIsWorking($this->connection);
        $this->assertSame(
            (string) getenv('SNOWFLAKE_DATABASE'),
            $this->connection->fetchOne('SELECT CURRENT_DATABASE()'),
        );
    }

    /**
     * @dataProvider connectionProvider
     */
    public function testWillDisconnect(Connection $wrappedConnection): void
    {
        $wrappedConnectionRef = new ReflectionClass($wrappedConnection);
        $wrappedConnectionPropRef = $wrappedConnectionRef->getProperty('_conn');
        $wrappedConnectionPropRef->setAccessible(true);
        $wrappedConnection->connect(); // create odbc resource
        /** @var SnowflakeConnection $snowflakeConnection */
        $snowflakeConnection = $wrappedConnectionPropRef->getValue($wrappedConnection);
        $snowflakeConnectionPropRef = new ReflectionClass($snowflakeConnection);
        $snowflakeConnectionPropRef = $snowflakeConnectionPropRef->getProperty('conn');
        $snowflakeConnectionPropRef->setAccessible(true);
        // check resource exists
        $this->assertIsResource($snowflakeConnectionPropRef->getValue($snowflakeConnection));
        $this->assertSame('odbc link', get_resource_type($snowflakeConnectionPropRef->getValue($snowflakeConnection)));
        $wrappedConnection->close();

        $this->assertNull($wrappedConnectionPropRef->getValue($wrappedConnection));
        // try reconnect
        $this->assertConnectionIsWorking($wrappedConnection);
        $wrappedConnection->close();
    }

    /**
     * @dataProvider connectionProvider
     */
    public function testQueryTagging(Connection $connection): void
    {
        $connection->executeQuery('SELECT current_date;');
        $queries = $connection->fetchAllAssociative(<<<SQL
    SELECT 
        QUERY_TEXT, QUERY_TAG 
    FROM 
        TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
    WHERE QUERY_TEXT = 'SELECT current_date;' 
    ORDER BY START_TIME DESC 
    LIMIT 1
SQL,);

        $this->assertEquals('{"runId":"runIdValue"}', $queries[0]['QUERY_TAG']);
    }

    /**
     * @dataProvider connectionProvider
     */
    public function testQueryTimeoutLimit(Connection $connection): void
    {
        $connection->executeStatement('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3');
        try {
            $connection->executeStatement('CALL system$wait(5)');
        } catch (WarehouseTimeoutReached $e) {
            $this->assertSame(WarehouseTimeoutReached::class, get_class($e));
            $this->assertSame(
                'An exception occurred while executing a query: Query reached its timeout 3 second(s)',
                $e->getMessage(),
            );
        } finally {
            $connection->executeStatement('ALTER SESSION UNSET STATEMENT_TIMEOUT_IN_SECONDS');
        }
    }

    /**
     * @dataProvider connectionProvider
     */
    public function testSchema(Connection $connection): void
    {

        $this->assertConnectionIsWorking($connection);
        $this->assertNull($connection->fetchOne('SELECT CURRENT_SCHEMA()'));
        $connection->close();

        $this->connection->executeStatement('CREATE SCHEMA IF NOT EXISTS "tableUtils-testSchema"');
        $connection = SnowflakeConnectionFactory::getConnection(
            (string) getenv('SNOWFLAKE_HOST'),
            (string) getenv('SNOWFLAKE_USER'),
            (string) getenv('SNOWFLAKE_PASSWORD'),
            [
                'port' => (string) getenv('SNOWFLAKE_PORT'),
                'warehouse' => (string) getenv('SNOWFLAKE_WAREHOUSE'),
                'database' => (string) getenv('SNOWFLAKE_DATABASE'),
                'schema' => 'tableUtils-testSchema',
            ],
        );
        //tests if you set schema in constructor it really set in connection
        $this->assertSame(
            'tableUtils-testSchema',
            $connection->fetchOne('SELECT CURRENT_SCHEMA()'),
        );
        $this->connection->close();
        $connection->close();

        //main connection has still no schema
        $this->assertNull(
            $this->connection->fetchOne('SELECT CURRENT_SCHEMA()'),
        );

        $this->expectException(Exception::class);
        $this->expectExceptionMessage(
            sprintf(
                'Cannot access object or it does not exist. Executing query "USE SCHEMA %s"',
                SnowflakeQuote::quoteSingleIdentifier('Other schema'),
            ),
        );
        $connection->executeQuery(sprintf('USE SCHEMA %s', SnowflakeQuote::quoteSingleIdentifier('Other schema')));
    }
}
