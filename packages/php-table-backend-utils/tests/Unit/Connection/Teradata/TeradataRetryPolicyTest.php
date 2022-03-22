<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Connection\Teradata;

use Doctrine\DBAL\Exception;
use Generator;
use Keboola\TableBackendUtils\Connection\Teradata\TeradataRetryPolicy;
use PHPUnit\Framework\TestCase;
use Retry\RetryContextInterface;

class TeradataRetryPolicyTest extends TestCase
{
    /**
     * @return Generator<string, array<string>>
     */
    public function shouldRetryProvider(): Generator
    {
        // phpcs:ignore
        yield 'Concurrent change conflict' => ['An exception occurred while executing a query: SQLSTATE[40001]: Serialization failure: -3598 [Teradata][ODBC Teradata Driver][Teradata Database](-3598)Concurrent change conflict on database -- try again. (SQLExecDirect[4294963698] at /usr/src/php/ext/pdo_odbc/odbc_driver.c:246)'];
    }

    /**
     * @dataProvider shouldRetryProvider
     */
    public function testShouldRetry(string $message): void
    {
        $policy = new TeradataRetryPolicy();
        $context = $this->createMock(RetryContextInterface::class);
        $context->method('getLastException')->willReturn(new Exception($message));
        $this->assertTrue($policy->canRetry($context));
    }

    public function testNoException(): void
    {
        $policy = new TeradataRetryPolicy();
        $context = $this->createMock(RetryContextInterface::class);
        $context->method('getLastException')->willReturn(null);
        $this->assertFalse($policy->canRetry($context));
    }

    public function testShouldNotRetry(): void
    {
        $policy = new TeradataRetryPolicy();
        $context = $this->createMock(RetryContextInterface::class);
        $context->method('getLastException')->willReturn(new Exception('Unknown exception should not retry.'));
        $this->assertFalse($policy->canRetry($context));
    }
}
