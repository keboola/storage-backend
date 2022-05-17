<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Synapse;

use Generator;
use Keboola\TableBackendUtils\Table\Synapse\TableDistributionDefinition;
use LogicException;
use PHPUnit\Framework\TestCase;

class TableDistributionDefinitionTest extends TestCase
{
    public function testValidReplicated(): void
    {
        $definition = new TableDistributionDefinition('REPLICATE', []);
        self::assertSame([], $definition->getDistributionColumnsNames());
        self::assertSame('REPLICATE', $definition->getDistributionName());
        self::assertFalse($definition->isHashDistribution());
    }

    public function testValidRoundRobin(): void
    {
        $definition = new TableDistributionDefinition('ROUND_ROBIN', []);
        self::assertSame([], $definition->getDistributionColumnsNames());
        self::assertSame('ROUND_ROBIN', $definition->getDistributionName());
        self::assertFalse($definition->isHashDistribution());
    }

    public function testValidHash(): void
    {
        $definition = new TableDistributionDefinition('HASH', ['id']);
        self::assertSame(['id'], $definition->getDistributionColumnsNames());
        self::assertSame('HASH', $definition->getDistributionName());
        self::assertTrue($definition->isHashDistribution());
    }

    /**
     * @return \Generator<string,array<int, string[]>>
     */
    public function invalidHashDistributionColumns(): Generator
    {
        yield 'More than one column' => [['id1', 'id2']];
        yield 'No column' => [[]];
    }

    /**
     * @param string[] $columns
     * @dataProvider invalidHashDistributionColumns
     */
    public function testInvalidHashColumns(array $columns): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('HASH table distribution must have one distribution key specified.');
        new TableDistributionDefinition('HASH', $columns);
    }
}
