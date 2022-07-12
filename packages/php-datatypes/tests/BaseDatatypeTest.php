<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Keboola\Datatype\Definition\BaseType;
use PHPUnit\Framework\TestCase;

class BaseDatatypeTest extends TestCase
{
    public function testIsValid(): void
    {
        $this->assertTrue(BaseType::isValid('BOOLEAN'));

        $this->assertFalse(BaseType::isValid('Boolean'));
        $this->assertFalse(BaseType::isValid('not-exist'));
    }
}
