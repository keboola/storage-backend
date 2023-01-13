<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\TestsStubLoader\Driver\Exception;

use Keboola\StorageDriver\Shared\Driver\Exception\Exception;
use PHPUnit\Framework\TestCase;

class ExceptionTest extends TestCase
{
    public function testIsRetryable(): void
    {
        $e = new Exception('test');
        $this->assertTrue($e->isRetryable());
    }

    public function testContext(): void
    {
        $e = new Exception('test');
        $this->assertSame([], $e->getContext());
        $e->setContext(['test']);
        $this->assertSame(['test'], $e->getContext());
    }
}
