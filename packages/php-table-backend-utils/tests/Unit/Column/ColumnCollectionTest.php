<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Column;

use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\ColumnInterface;
use Keboola\TableBackendUtils\Column\Exasol\ExasolColumn;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Column\SynapseColumn;
use Keboola\TableBackendUtils\Column\Teradata\TeradataColumn;
use Keboola\TableBackendUtils\ColumnException;
use PHPUnit\Framework\TestCase;

class ColumnCollectionTest extends TestCase
{
    public function testGetIterator(): void
    {
        $collection = new ColumnCollection([
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ]);

        $this->assertIsIterable($collection);
    }

    /**
     * @dataProvider  tooMuchColumnsProviderWithLimits
     * @param SynapseColumn|TeradataColumn $definitionClass
     */
    public function testTooMuchColumns($definitionClass, int $limit): void
    {
        $cols = [];
        for ($i = 0; $i < $limit + 2; $i++) {
            $cols[] = $definitionClass::createGenericColumn('name' . $i);
        }

        $this->expectException(ColumnException::class);
        $this->expectExceptionMessage(sprintf('Too many columns. Maximum is %s columns.', $limit - 1));
        new ColumnCollection($cols);
    }

    /**
     * @dataProvider  tooMuchColumnsProviderWithNoLimits
     * @param SnowflakeColumn|ExasolColumn $definitionClass
     */
    public function testNoColumnsLimit($definitionClass, int $limit): void
    {
        $cols = [];
        for ($i = 0; $i < $limit + 2; $i++) {
            $cols[] = $definitionClass::createGenericColumn('name' . $i);
        }

        $this->expectNotToPerformAssertions();
        new ColumnCollection($cols);
    }

    public function testCount(): void
    {
        $collection = new ColumnCollection([
            SynapseColumn::createGenericColumn('col1'),
            SynapseColumn::createGenericColumn('col2'),
        ]);
        $this->assertCount(2, $collection);
    }

    /**
     * @return array<string, array{string, int}>
     */
    public function tooMuchColumnsProviderWithLimits(): array
    {
        return
            [
                'synapse' => [
                    SynapseColumn::class,
                    1024,
                ],
                'teradata' => [
                    TeradataColumn::class,
                    2048,
                ],
                'snowflake' => [
                    SnowflakeColumn::class,
                    1201,
                ],
            ];
    }

    /**
     * @return array<string, array{string, int}>
     */
    public function tooMuchColumnsProviderWithNoLimits(): array
    {
        return
            [
                'exasol' => [
                    ExasolColumn::class,
                    5000,
                ],
            ];
    }
}
