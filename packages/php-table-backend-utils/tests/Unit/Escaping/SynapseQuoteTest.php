<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Escaping;

use Generator;
use Keboola\TableBackendUtils\Escaping\SynapseQuote;
use PHPUnit\Framework\TestCase;

class SynapseQuoteTest extends TestCase
{
    /**
     * @return \Generator<string, array<int, string>>
     */
    public function valueQuoteProvider(): Generator
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
        self::assertSame($expectedOutput, SynapseQuote::quote($value));
    }

    /**
     * @return \Generator<string, array<int, string>>
     */
    public function valueQuoteIdentifierProvider(): Generator
    {
        yield 'simple' => [
            'valueToQuote',
            '[valueToQuote]',
        ];
        yield 'simple2' => [
            'abc[]def',
            '[abc[]]def]',
        ];
        yield 'with escaping char' => [
            'value\'To\'Quote',
            '[value\'To\'Quote]',
        ];
        yield 'complex' => [
            'va[l.u]e\']To\'[Q][uo.t[]e',
            '[va[l.u]]e\']]To\'[Q]][uo.t[]]e]',
        ];
    }


    /**
     * @dataProvider valueQuoteIdentifierProvider
     */
    public function testQuoteSingleIdentifier(string $value, string $expectedOutput): void
    {
        self::assertSame($expectedOutput, SynapseQuote::quoteSingleIdentifier($value));
    }
}
