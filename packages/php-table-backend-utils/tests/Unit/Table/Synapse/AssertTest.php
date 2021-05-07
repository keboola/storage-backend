<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Table\Synapse;

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
    public function validIndexes(): \Generator
    {
        yield 'CLUSTERED COLUMNSTORE INDEX' => ['CLUSTERED COLUMNSTORE INDEX'];
        yield 'HEAP' => ['HEAP'];
        yield 'CLUSTERED INDEX' => ['CLUSTERED INDEX'];
    }

    public function testAssertInvalidTableIndex(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Unknown table index type: "Invalid index" specified.');
        Assert::assertTableIndex('Invalid index');
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
    public function validDistributions(): \Generator
    {
        yield 'HASH' => ['HASH'];
        yield 'REPLICATE' => ['REPLICATE'];
        yield 'ROUND_ROBIN' => ['ROUND_ROBIN'];
    }

    public function testAssertInvalidTableDistribution(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Unknown table distribution: "Invalid index" specified.');
        Assert::assertTableDistribution('Invalid index');
    }

    /**
     * @return \Generator<string,array<int, string[]>>
     */
    public function invalidHashDistributions(): \Generator
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
