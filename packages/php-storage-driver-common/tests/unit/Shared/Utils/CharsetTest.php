<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\Utils;

use Keboola\StorageDriver\Shared\Utils\Charset;
use PHPUnit\Framework\TestCase;

class CharsetTest extends TestCase
{
    public function testGetCharlistFromRange(): void
    {
        $this->assertEquals(
            '0123456789',
            Charset::getCharlistFromRange('0-9')
        );
        $this->assertEquals(
            'abcdefghijklmnopqrstuvwxyzABCDEF',
            Charset::getCharlistFromRange('a-zA-F')
        );
    }
}
