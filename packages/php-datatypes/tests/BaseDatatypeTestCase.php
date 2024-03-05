<?php

declare(strict_types=1);

namespace Keboola\DatatypeTest;

use Generator;
use Keboola\Datatype\Definition\BaseType;
use Keboola\Datatype\Definition\DefinitionInterface;
use Keboola\Datatype\Definition\Exception\InvalidTypeException;
use PHPUnit\Framework\TestCase;

abstract class BaseDatatypeTestCase extends TestCase
{
    /**
     * @return class-string<DefinitionInterface>
     */
    abstract public static function getTestedClass(): string;

    /**
     * @return Generator<array{
     *     basetype: string,
     *     expectedType: string|null,
     *     expectToFail?: bool,
     * }>
     */
    abstract public static function provideTestGetTypeByBasetype(): Generator;

    /**
     * @return Generator<array{
     *     basetype: string,
     *     expectedColumnDefinition: DefinitionInterface|null,
     *     expectToFail?: bool,
     * }>
     */
    abstract public static function provideTestGetDefinitionForBasetype(): Generator;

    /**
     * @dataProvider provideTestGetTypeByBasetype
     */
    public function testGetTypeByBasetype(
        string $basetype,
        ?string $expectedType = null,
        bool $expectToFail = false,
    ): void {
        if ($expectedType === null) {
            $this->assertTrue($expectToFail, 'When $expectedType is null, test must expect to fail.');
            $this->expectException(InvalidTypeException::class);
        }
        $this->assertSame(
            $expectedType,
            static::getTestedClass()::getTypeByBasetype($basetype),
        );
    }

    /**
     * @dataProvider provideTestGetDefinitionForBasetype
     */
    public function testGetDefinitionForBasetype(
        string $basetype,
        ?DefinitionInterface $expectedColumnDefinition = null,
        bool $expectToFail = false,
    ): void {
        if ($expectedColumnDefinition === null) {
            $this->assertTrue($expectToFail, 'When $expectedType is null, test must expect to fail.');
            $this->expectException(InvalidTypeException::class);
        }
        $definition = static::getTestedClass()::getDefinitionForBasetype($basetype)->toArray();
        $this->assertInstanceOf(DefinitionInterface::class, $expectedColumnDefinition);
        $this->assertSame(
            $expectedColumnDefinition->toArray(),
            $definition,
        );
    }

    public function testAllBaseTypesTestedGetTypeByBasetype(): void
    {
        $this->assertBaseTypesTested(array_map(
            fn(array $case) => $case['basetype'],
            iterator_to_array(static::provideTestGetTypeByBasetype()),
        ));
    }

    public function testAllBaseTypesTestedGetDefinitionForBasetype(): void
    {
        $this->assertBaseTypesTested(array_map(
            fn(array $case) => $case['basetype'],
            iterator_to_array(static::provideTestGetDefinitionForBasetype()),
        ));
    }

    /**
     * @param string[] $tested
     */
    protected function assertBaseTypesTested(array $tested): void
    {
        $difference = array_diff(BaseType::TYPES, $tested);
        $this->assertEmpty(
            $difference,
            'Not all base types are tested. Missing: ' . implode(', ', $difference),
        );
    }
}
