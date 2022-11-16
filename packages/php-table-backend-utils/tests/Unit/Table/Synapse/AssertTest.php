<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Synapse;

use Generator;
use Keboola\TableBackendUtils\Table\Synapse\Assert;
use LogicException;
use PHPUnit\Framework\TestCase;

class AssertTest extends TestCase
{
    /**
     * @dataProvider validIndexes
     * @doesNotPerformAssertions
     */
    public function testAssertTableIndex(string $indexName): void
    {
        Assert::assertTableIndex($indexName);
    }

    /**
     * @return \Generator<string,array<int,string>>
     */
    public function validIndexes(): Generator
    {
        yield 'CLUSTERED COLUMNSTORE INDEX' => ['CLUSTERED COLUMNSTORE INDEX'];
        yield 'HEAP' => ['HEAP'];
        yield 'CLUSTERED INDEX' => ['CLUSTERED INDEX'];
    }

    public function testAssertInvalidTableIndex(): void
    {
        $this->expectException(LogicException::class);
        // phpcs:ignore
        $this->expectExceptionMessage('Unknown table index type: "Invalid index" specified. Available types are CLUSTERED COLUMNSTORE INDEX|HEAP|CLUSTERED INDEX.');
        Assert::assertTableIndex('Invalid index');
    }

    /**
     * @return \Generator<string,array<int, string[]>>
     */
    public function invalidClusteredIndexes(): Generator
    {
        yield 'No column' => [[]];
    }

    /**
     * @param string[] $columns
     * @dataProvider invalidClusteredIndexes
     */
    public function testAssertInvalidClusteredIndex(array $columns): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('CLUSTERED table index must have at least one key column specified.');
        Assert::assertValidClusteredIndex('CLUSTERED INDEX', $columns);
    }

    /**
     * @doesNotPerformAssertions
     */
    //phpcs:ignore
    public function testAssertValidClusteredIndex(): void
    {
        Assert::assertValidClusteredIndex('CLUSTERED INDEX', ['id1']);
    }

    /**
     * @dataProvider validDistributions
     * @doesNotPerformAssertions
     */
    public function testAssertTableDistribution(string $indexName): void
    {
        Assert::assertTableDistribution($indexName);
    }

    /**
     * @return \Generator<string,array<int, string>>
     */
    public function validDistributions(): Generator
    {
        yield 'HASH' => ['HASH'];
        yield 'REPLICATE' => ['REPLICATE'];
        yield 'ROUND_ROBIN' => ['ROUND_ROBIN'];
    }

    public function testAssertInvalidTableDistribution(): void
    {
        $this->expectException(LogicException::class);
        // phpcs:ignore
        $this->expectExceptionMessage('Unknown table distribution: "Invalid index" specified. Available distributions are HASH|REPLICATE|ROUND_ROBIN.');
        Assert::assertTableDistribution('Invalid index');
    }

    /**
     * @return \Generator<string,array<int, string[]>>
     */
    public function invalidHashDistributions(): Generator
    {
        yield 'More than one column' => [['id1', 'id2']];
        yield 'No column' => [[]];
    }

    /**
     * @param string[] $columns
     * @dataProvider invalidHashDistributions
     */
    public function testAssertInvalidHashDistribution(array $columns): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('HASH table distribution must have one distribution key specified.');
        Assert::assertValidHashDistribution('HASH', $columns);
    }

    /**
     * @doesNotPerformAssertions
     */
    //phpcs:ignore
    public function testAssertValidHashDistribution(): void
    {
        Assert::assertValidHashDistribution('HASH', ['id1']);
    }
}
