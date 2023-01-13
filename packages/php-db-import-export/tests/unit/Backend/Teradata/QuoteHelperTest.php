<?php

declare(strict_types=1);

namespace Tests\Keboola\Db\ImportExportUnit\Backend\Teradata;

use Generator;
use Keboola\Db\ImportExport\Backend\Teradata\Helper\QuoteHelper;
use PHPUnit\Framework\TestCase;

class QuoteHelperTest extends TestCase
{
    /**
     * @return Generator<string, array{string, string}>
     */
    public function quoteValuesProvider(): Generator
    {
        yield 'simple value' => [
            '\'value\'',
            'value',
        ];
        yield 'value with quote' => [
            '\'val\\\'ue\'',
            'val\'ue',
        ];
    }

    /**
     * @dataProvider quoteValuesProvider
     */
    public function testQuoteValue(string $expected, string $value): void
    {
        $this->assertSame($expected, QuoteHelper::quoteValue($value));
    }
}
