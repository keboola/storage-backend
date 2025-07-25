<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Connection\Bigquery;

use InvalidArgumentException;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTagKey;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTags;
use PHPUnit\Framework\TestCase;

class QueryTagsTest extends TestCase
{
    public function testEmptyLabels(): void
    {
        $queryTags = new QueryTags();
        $this->assertTrue($queryTags->isEmpty());
        $this->assertEmpty($queryTags->toArray());
    }

    public function testValidLabelInConstructor(): void
    {
        $queryTags = new QueryTags([
            QueryTagKey::BRANCH_ID->value => 'test-branch',
        ]);

        $this->assertFalse($queryTags->isEmpty());
        $this->assertEquals(
            ['branch_id' => 'test-branch'],
            $queryTags->toArray(),
        );
    }

    public function testValidLabelAddition(): void
    {
        $queryTags = new QueryTags();
        $queryTags->addLabel(QueryTagKey::BRANCH_ID->value, 'test-branch');

        $this->assertFalse($queryTags->isEmpty());
        $this->assertEquals(
            ['branch_id' => 'test-branch'],
            $queryTags->toArray(),
        );
    }

    public function testInvalidLabelInConstructor(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid label key "invalid_key". Valid keys are: branch_id');

        new QueryTags([
            'invalid_key' => 'some-value',
        ]);
    }

    public function testInvalidLabelAddition(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid label key "invalid_key". Valid keys are: branch_id');

        $queryTags = new QueryTags();
        $queryTags->addLabel('invalid_key', 'some-value');
    }

    public function testChainedLabelAddition(): void
    {
        $queryTags = new QueryTags();
        $queryTags
            ->addLabel(QueryTagKey::BRANCH_ID->value, 'test-branch-1')
            ->addLabel(QueryTagKey::BRANCH_ID->value, 'test-branch-2'); // Override previous value

        $this->assertEquals(
            ['branch_id' => 'test-branch-2'],
            $queryTags->toArray(),
        );
    }

    /**
     * @dataProvider validLabelValuesProvider
     */
    public function testValidLabelValues(string $value): void
    {
        $queryTags = new QueryTags();
        $queryTags->addLabel(QueryTagKey::BRANCH_ID->value, $value);
        $this->assertEquals($value, $queryTags->toArray()[QueryTagKey::BRANCH_ID->value]);
    }

    /**
     * @return array<string, array{string}>
     */
    public function validLabelValuesProvider(): array
    {
        return [
            'empty value' => [''],
            'simple value' => ['test'],
            'with numbers' => ['test123'],
            'with underscore' => ['test_branch'],
            'with dash' => ['test-branch'],
            'complex value' => ['my-test-branch-123'],
            'single letter' => ['a'],
            'starts with number' => ['123-test'],
            'starts with underscore' => ['_test'],
            'starts with dash' => ['-test'],
            'international characters' => ['čeština-test'],
            'international single char' => ['ě'],
            'mixed international' => ['test-über-123'],
        ];
    }

    /**
     * @dataProvider invalidLabelValuesProvider
     */
    public function testInvalidLabelValues(string $value, string $expectedMessage): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedMessage);

        $queryTags = new QueryTags();
        $queryTags->addLabel(QueryTagKey::BRANCH_ID->value, $value);
    }

    /**
     * @return array<string, array{string, string}>
     */
    public function invalidLabelValuesProvider(): array
    {
        return [
            'uppercase letters' => [
                'Test-Branch',
                'Invalid label value "Test-Branch". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
            'special characters' => [
                'test@branch',
                'Invalid label value "test@branch". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
            'too long' => [
                str_repeat('a', 64),
                'Label value "' . str_repeat('a', 64) . '" is too long. Maximum length is 63 characters.',
            ],
            'uppercase international' => [
                'ÜBER-test',
                'Invalid label value "ÜBER-test". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
            'special international' => [
                'test-€-symbol',
                'Invalid label value "test-€-symbol". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
        ];
    }
}
