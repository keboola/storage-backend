<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\UnitTests\Shared\Utils;

use Keboola\StorageDriver\Shared\Utils\Password;
use Keboola\StorageDriver\Shared\Utils\StringUtils;
use PHPUnit\Framework\TestCase;

class StringUtilsTest extends TestCase
{
    public function testRandomizeString(): void
    {
        $string = Password::generate(100);

        $randomString1 = str_split(StringUtils::shuffle($string));
        sort($randomString1);
        $randomString2 = str_split(StringUtils::shuffle($string));
        sort($randomString2);

        $this->assertEquals(
            $randomString1,
            $randomString2,
        );
    }
}
