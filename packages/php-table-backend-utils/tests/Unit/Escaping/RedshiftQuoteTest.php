<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Escaping;

use Generator;
use Keboola\TableBackendUtils\Escaping\RedshiftQuote;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

class RedshiftQuoteTest extends TestCase
{
    /**
     * @return \Generator<string, array<int, string>>
     */
    public static function valueQuoteProvider(): Generator
    {
        yield 'simple' => [
            'valueToQuote',
            '\'valueToQuote\'',
        ];
        yield 'with escaping char' => [
            'value\'To\'Quote',
            '\'value\'\'To\'\'Quote\'',
        ];
        yield 'complex' => [
            'va[l.u]e\']To\'[Q]"[uo.t"[]e',
            '\'va[l.u]e\'\']To\'\'[Q]"[uo.t"[]e\'',
        ];
    }

    #[DataProvider('valueQuoteProvider')]
    public function testQuote(string $value, string $expectedOutput): void
    {
        self::assertSame($expectedOutput, RedshiftQuote::quote($value));
    }

    /**
     * @return \Generator<string, array<int, string>>
     */
    public static function valueQuoteIdentifierProvider(): Generator
    {
        yield 'simple' => [
            'valueToQuote',
            '"valueToQuote"',
        ];
        yield 'with escaping char' => [
            'value\'To\'Quote',
            '"value\'To\'Quote"',
        ];
        yield 'complex' => [
            'va[l.u]e\']To\'[Q]"[uo.t"[]e',
            '"va[l.u]e\']To\'[Q]""[uo.t""[]e"',
        ];
    }


    #[DataProvider('valueQuoteIdentifierProvider')]
    public function testQuoteSingleIdentifier(string $value, string $expectedOutput): void
    {
        self::assertSame($expectedOutput, RedshiftQuote::quoteSingleIdentifier($value));
    }
}
