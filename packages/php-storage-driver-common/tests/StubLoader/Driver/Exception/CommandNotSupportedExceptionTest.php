<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\TestsStubLoader\Driver\Exception;

use Keboola\StorageDriver\Shared\Driver\Exception\CommandNotSupportedException;
use PHPUnit\Framework\TestCase;

class CommandNotSupportedExceptionTest extends TestCase
{
    public function testIsRetryable(): void
    {
        $e = new CommandNotSupportedException('test');
        $this->assertFalse($e->isRetryable());
    }
}
