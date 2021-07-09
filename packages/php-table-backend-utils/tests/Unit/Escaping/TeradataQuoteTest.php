<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Escaping;

use Keboola\TableBackendUtils\Escaping\Teradata\TeradataQuote;
use PHPUnit\Framework\TestCase;

class TeradataQuoteTest extends TestCase
{
    /**
     * @return \Generator<string, array<int, string>>
     */
    public function valueQuoteProvider(): \Generator
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
            'va[l.u]e\']To\'[Q][uo.t[]e',
            '\'va[l.u]e\'\']To\'\'[Q][uo.t[]e\'',
        ];
    }

    /**
     * @dataProvider valueQuoteProvider
     */
    public function testQuote(string $value, string $expectedOutput): void
    {
        self::assertSame($expectedOutput, TeradataQuote::quote($value));
    }

    /**
     * @return \Generator<string, array<int, string>>
     */
    public function valueQuoteIdentifierProvider(): \Generator
    {
        yield 'simple' => [
            'valueToQuote',
            '"valueToQuote"',
        ];
        yield 'with quote in indentifier' => [
            'abc"def',
            '"abc""def"',
        ];
        yield 'with escaping char' => [
            'value\"ToQuote',
            '"value\""ToQuote"',
        ];
    }


    /**
     * @dataProvider valueQuoteIdentifierProvider
     */
    public function testQuoteSingleIdentifier(string $value, string $expectedOutput): void
    {
        self::assertSame($expectedOutput, TeradataQuote::quoteSingleIdentifier($value));
    }
}
