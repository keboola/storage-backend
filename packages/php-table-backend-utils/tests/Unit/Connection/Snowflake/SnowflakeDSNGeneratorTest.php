<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Connection\Snowflake;

use Generator;
use Keboola\TableBackendUtils\Connection\Exception\DriverException;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeDSNGenerator;
use LogicException;
use PHPUnit\Framework\TestCase;

class SnowflakeDSNGeneratorTest extends TestCase
{
    /**
     * @return \Generator<array<int, array<string, array<mixed>|bool|int|string>|string>>
     */
    public function dsnOptionsProvider(): Generator
    {
        $options = [
            'host' => 'snowflakecomputing.com',
            'user' => 'snowflake_user',
            'password' => 'snowflake_user',
            'port' => 123,
            'tracing' => 1,
            'loginTimeout' => 10,
            'networkTimeout' => 15,
            'queryTimeout' => 20,
            'maxBackoffAttempts' => 8,
            'database' => 'snowflake_db',
            'schema' => 'snowflake_schema',
            'warehouse' => 'snowflake_warehouse',
            'runId' => 'snowflake_runId',
            'clientSessionKeepAlive' => true,
            'driverClass' => 'DBAL\DRIVER',
            'driverOptions' => [],
        ];

        yield 'all' => [
            $options,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['port']);
        yield 'no port' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=443;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['tracing']);
        yield 'no tracing' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=0;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['loginTimeout']);
        yield 'no loginTimeout' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['networkTimeout']);
        yield 'no networkTimeout' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['queryTimeout']);
        yield 'no queryTimeout' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['maxBackoffAttempts']);
        yield 'no maxBackoffAttempts' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['database']);
        yield 'no database' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['schema']);
        yield 'no schema' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['warehouse']);
        yield 'no warehouse' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['clientSessionKeepAlive']);
        yield 'no clientSessionKeepAlive' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        $optionsCopy['port'] = '';
        yield 'empty string as port' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=443;Tracing=1;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];

        $optionsCopy = $options;
        unset($optionsCopy['password']);
        $optionsCopy['privateKeyPath'] = 'c:/path/to/private.key';
        yield 'cert' => [
            $optionsCopy,
            // phpcs:ignore
            'Driver=SnowflakeDSIIDriver;Server=snowflakecomputing.com;Port=123;Tracing=1;AUTHENTICATOR=SNOWFLAKE_JWT;PRIV_KEY_FILE=c:/path/to/private.key;UID=snowflake_user;Login_timeout=10;Network_timeout=15;Query_timeout=20;Database="snowflake_db";Schema="snowflake_schema";Warehouse="snowflake_warehouse";CLIENT_SESSION_KEEP_ALIVE=TRUE;application="Keboola_Connection"',
        ];
    }

    /**
     * @dataProvider dsnOptionsProvider
     * @param array<mixed> $options
     */
    public function testDSNGenerate(array $options, string $expectedDSN): void
    {
        // @phpstan-ignore-next-line
        $this->assertSame($expectedDSN, SnowflakeDSNGenerator::generateDSN($options));
    }

    public function testFailsToConnectWithUnknownParams(): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('Unknown options: someRandomParameter, otherRandomParameter, 0');

        // @phpstan-ignore-next-line
        SnowflakeDSNGenerator::generateDSN([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => getenv('SNOWFLAKE_PASSWORD'),
            'someRandomParameter' => false,
            'otherRandomParameter' => false,
            'value',
        ]);
    }

    public function testFailsWhenRequiredParamsMissing(): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('Missing options: host');

        // @phpstan-ignore-next-line
        SnowflakeDSNGenerator::generateDSN([
            'password' => getenv('SNOWFLAKE_PASSWORD'),
        ]);
    }

    public function testFailsWhenPasswordAndCertMissing(): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('Either "password" or "privateKeyPath" must be provided.');

        // @phpstan-ignore-next-line
        SnowflakeDSNGenerator::generateDSN([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
        ]);
    }

    public function testFailsWhenPasswordAndCertBothIsSet(): void
    {
        $this->expectException(DriverException::class);
        $this->expectExceptionMessage('Both "password" and "privateKeyPath" cannot be set at the same time.');

        // @phpstan-ignore-next-line
        SnowflakeDSNGenerator::generateDSN([
            'host' => getenv('SNOWFLAKE_HOST'),
            'user' => getenv('SNOWFLAKE_USER'),
            'password' => 'pass',
            'privateKeyPath' => 'cert',
        ]);
    }
}
