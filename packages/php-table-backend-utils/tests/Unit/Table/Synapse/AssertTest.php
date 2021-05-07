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
     * @return \Generator<string,string>
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
}
