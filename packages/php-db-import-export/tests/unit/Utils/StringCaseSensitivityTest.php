<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Utils;

use Keboola\Db\ImportExport\Utils\StringCaseSensitivity;
use PHPUnit\Framework\TestCase;

class StringCaseSensitivityTest extends TestCase
{
    public function testStringToLower(): void
    {
        $string = 'testMe_man';
        $result = StringCaseSensitivity::stringToLower($string);
        $this->assertSame('testme_man', $result);
    }

    public function testArrayToLower(): void
    {
        $string = ['testMe_man', 'COLUMN_X', 'CoLuMn_xYZ-0'];
        $result = StringCaseSensitivity::arrayToLower($string);
        $this->assertSame(['testme_man', 'column_x', 'column_xyz-0'], $result);
    }

    public function testIsEqualCaseInsensitive(): void
    {
        $this->assertTrue(StringCaseSensitivity::isEqualCaseInsensitive('TIMESTAMP', 'timestamp'));
        $this->assertTrue(StringCaseSensitivity::isEqualCaseInsensitive('Timestamp', 'timestamp'));
        $this->assertTrue(StringCaseSensitivity::isEqualCaseInsensitive('timestamp', 'timestamp'));
        $this->assertTrue(StringCaseSensitivity::isEqualCaseInsensitive('TIMEsTAMP', 'timestamp'));
        $this->assertTrue(StringCaseSensitivity::isEqualCaseInsensitive('tImEsTaMp', 'timestamp'));

        $this->assertFalse(StringCaseSensitivity::isEqualCaseInsensitive('_timestamp', 'timestamp'));
    }

    public function testIsInArrayCaseInsensitive(): void
    {
        $needle = 'COLUMN_XYZ-0';
        $haystack = ['testMe_man', 'COLUMN_X', 'CoLuMn_xYZ-0'];

        $this->assertTrue(StringCaseSensitivity::isInArrayCaseInsensitive($needle, $haystack));

        $needle = 'COLUMN_XYZ_0';
        $this->assertFalse(StringCaseSensitivity::isInArrayCaseInsensitive($needle, $haystack));
    }
}
