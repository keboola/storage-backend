<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit;

use Exception;
use Keboola\TableBackendUtils\DataHelper;
use Keboola\TableBackendUtils\Table\TableStats;
use PHPUnit\Framework\TestCase;

/**
 * @covers TableStats
 */
class DataHelperTest extends TestCase
{
    /**
     * @param mixed[] $inputData
     * @param string $key
     * @param mixed[] $expectedResult
     * @throws Exception
     * @dataProvider  validDataProvider
     */
    public function testExtractByKey(array $inputData, string $key, array $expectedResult): void
    {
        self::assertSame($expectedResult, DataHelper::extractByKey($inputData, $key));
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function validDataProvider(): \Generator
    {
        yield '1dArray' => [
            'inputData' => [
                ['ColumnName' => 'a'],
                ['ColumnName' => 1],
                ['ColumnName' => ['c']],
            ],
            'key' => 'ColumnName',
            'exceptionResult' => ['a', 1, ['c']],
        ];
    }

    /**
     * @param mixed[] $inputData
     * @param string $key
     * @param string $expectedMessage
     * @throws Exception
     * @dataProvider  invalidDataProvider
     */
    public function testExtractByKeyWithInvalidData(array $inputData, string $key, string $expectedMessage): void
    {
        $this->expectExceptionMessage($expectedMessage);
        DataHelper::extractByKey($inputData, $key);
    }

    /**
     * @return \Generator<string, mixed, mixed, mixed>
     */
    public function invalidDataProvider(): \Generator
    {
        yield 'trimming when input is array' => [
            'inputData' => [
                ['ColumnName' => ['a']],
                ['ColumnNameNotExisting' => ['b']],
                ['ColumnName' => ['c']],
            ],
            'key' => 'ColumnName',
            'exceptionResult' => 'Key ColumnName is not defined in array',
        ];
    }
}
