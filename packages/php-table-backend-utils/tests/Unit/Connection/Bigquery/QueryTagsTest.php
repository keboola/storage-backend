<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Connection\Bigquery;

use InvalidArgumentException;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTagKey;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTags;
use PHPUnit\Framework\TestCase;

class QueryTagsTest extends TestCase
{
    public function testEmptyQueryTags(): void
    {
        $queryTags = new QueryTags();
        $this->assertTrue($queryTags->isEmpty());
        $this->assertEmpty($queryTags->toArray());
    }

    public function testValidQueryTagsInConstructor(): void
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

    public function testValidQueryTagsAddition(): void
    {
        $queryTags = new QueryTags();
        $queryTags->addTag(QueryTagKey::BRANCH_ID->value, 'test-branch');

        $this->assertFalse($queryTags->isEmpty());
        $this->assertEquals(
            ['branch_id' => 'test-branch'],
            $queryTags->toArray(),
        );
    }

    public function testInvalidQueryTagsInConstructor(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid query tag key "invalid_key". Valid keys are: branch_id');

        new QueryTags([
            'invalid_key' => 'some-value',
        ]);
    }

    public function testInvalidQueryTagsAddition(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid query tag key "invalid_key". Valid keys are: branch_id');

        $queryTags = new QueryTags();
        $queryTags->addTag('invalid_key', 'some-value');
    }

    public function testChainedQueryTagsAddition(): void
    {
        $queryTags = new QueryTags();
        $queryTags
            ->addTag(QueryTagKey::BRANCH_ID->value, 'test-branch-1')
            ->addTag(QueryTagKey::BRANCH_ID->value, 'test-branch-2'); // Override previous value

        $this->assertEquals(
            ['branch_id' => 'test-branch-2'],
            $queryTags->toArray(),
        );
    }

    /**
     * @dataProvider validQueryTagsValuesProvider
     */
    public function testValidQueryTagsValues(string $value): void
    {
        $queryTags = new QueryTags();
        $queryTags->addTag(QueryTagKey::BRANCH_ID->value, $value);
        $this->assertEquals($value, $queryTags->toArray()[QueryTagKey::BRANCH_ID->value]);
    }

    /**
     * @return array<string, array{string}>
     */
    public function validQueryTagsValuesProvider(): array
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
     * @dataProvider invalidQueryTagsValuesProvider
     */
    public function testInvalidQueryTagsValues(string $value, string $expectedMessage): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($expectedMessage);

        $queryTags = new QueryTags();
        $queryTags->addTag(QueryTagKey::BRANCH_ID->value, $value);
    }

    /**
     * @return array<string, array{string, string}>
     */
    public function invalidQueryTagsValuesProvider(): array
    {
        return [
            'uppercase letters' => [
                'Test-Branch',
                'Invalid query tag value "Test-Branch" for key "branch_id". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
            'special characters' => [
                'test@branch',
                'Invalid query tag value "test@branch" for key "branch_id". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
            'too long' => [
                str_repeat('a', 64),
                'Query tag value "' . str_repeat('a', 64)
                . '" for key "branch_id" is too long. Maximum length is 63 characters.',
            ],
            'uppercase international' => [
                'ÜBER-test',
                'Invalid query tag value "ÜBER-test" for key "branch_id". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
            'special international' => [
                'test-€-symbol',
                'Invalid query tag value "test-€-symbol" for key "branch_id". Values can only contain lowercase letters'
                .' (including international characters), numbers, underscores and dashes.',
            ],
        ];
    }
}
