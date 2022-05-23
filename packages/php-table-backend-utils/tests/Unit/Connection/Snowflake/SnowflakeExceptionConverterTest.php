<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Connection\Snowflake;

use Doctrine\DBAL\Query;
use Generator;
use Keboola\TableBackendUtils\Connection\Exception\ConnectionException;
use Keboola\TableBackendUtils\Connection\Exception\DriverException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\CannotAccessObjectException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\StringTooLongException;
use Keboola\TableBackendUtils\Connection\Snowflake\Exception\WarehouseTimeoutReached;
use Keboola\TableBackendUtils\Connection\Snowflake\SnowflakeExceptionConverter;
use PHPUnit\Framework\TestCase;

class SnowflakeExceptionConverterTest extends TestCase
{
    /**
     * @return \Generator<string, array<mixed>>
     */
    public function exceptionProvider(): Generator
    {
        yield 'Incorrect username' => [
            ConnectionException::class,
            'An exception occurred in the driver: Incorrect username or password was specified.',
            'odbc_connect(): SQL error: Incorrect username or password was specified., SQL state 28000 in SQLConnect',
            2,
        ];

        yield 'String is too long' => [
            StringTooLongException::class,
            // phpcs:ignore
            'An exception occurred while executing a query: String \'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\' cannot be inserted because it\'s bigger than column size',
            // phpcs:ignore
            'String \'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\' is too long and would be truncated',
            22000,
            // phpcs:ignore
            'INSERT INTO "utilsTest_refTableSchema"."utilsTest_refTab" VALUES (1, \'franta\', \'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\')',
        ];

        yield 'Statement timeout' => [
            WarehouseTimeoutReached::class,
            'An exception occurred while executing a query: Query reached its timeout 3 second(s)',
            'Statement reached its statement or warehouse timeout of 3 second(s) and was canceled.',
            57014,
            'CALL system$wait(5)',
        ];

        yield 'Cannot access object' => [
            CannotAccessObjectException::class,
            // phpcs:ignore
            'An exception occurred while executing a query: Cannot access object or it does not exist. Executing query "USE SCHEMA "Other schema""',
            'SQL compilation error:\nObject does not exist, or operation cannot be performed.',
            2000,
            'USE SCHEMA "Other schema"',
        ];
    }

    /**
     * @dataProvider exceptionProvider
     *
     * @param class-string $expectedException
     */
    public function testConvert(
        string $expectedException,
        string $expectedExceptionMessage,
        string $inputExceptionMessage,
        int $inputExceptionCode = 0,
        ?string $inputSQL = null
    ): void {
        $exceptionThrown = new DriverException(
            $inputExceptionMessage,
            null,
            $inputExceptionCode,
        );
        $query = null;
        if ($inputSQL !== null) {
            $query = new Query($inputSQL, [], []);
        }

        $converter = new SnowflakeExceptionConverter();
        $convertedException = $converter->convert($exceptionThrown, $query);
        $this->assertInstanceOf($expectedException, $convertedException);
        $this->assertSame($expectedExceptionMessage, $convertedException->getMessage());
    }
}
