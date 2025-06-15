<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional;

use PHPUnit\Framework\TestCase;

class FooTest extends TestCase
{
    /**
     * @dataProvider barProvider
     */
    public function testBar($a, $b, $expected): void
    {
        $sum = $a + $b;

        $this->assertSame($expected, $sum);
    }

    public static function barProvider(): array
    {
        return [
            [13, 29, 42],
            [111, 555, 666],
        ];
    }
}
