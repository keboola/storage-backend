<?php

declare(strict_types=1);

namespace Tests\Keboola\TableBackendUtils\Unit\Connection\Bigquery;

use InvalidArgumentException;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTagKey;
use Keboola\TableBackendUtils\Connection\Bigquery\QueryTags;
use PHPUnit\Framework\Attributes\DataProvider;
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
            ],);

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
        $this->expectExceptionMessage(
            'Invalid query tag key "invalid_key".'
            . ' Valid keys are: branch_id, run_id, keboola_run_id, keboola_branch_id, keboola_service',
        );

        new QueryTags([
            'invalid_key' => 'some-value',
            ],);
    }

    public function testInvalidQueryTagsAddition(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Invalid query tag key "invalid_key".'
            . ' Valid keys are: branch_id, run_id, keboola_run_id, keboola_branch_id, keboola_service',
        );

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

    public function testKeboolaPrefixedTags(): void
    {
        $queryTags = new QueryTags([
            QueryTagKey::KEBOOLA_RUN_ID->value => 'test-run-123',
            QueryTagKey::KEBOOLA_BRANCH_ID->value => 'test-branch-456',
            QueryTagKey::KEBOOLA_SERVICE->value => 'sapi',
        ]);

        $this->assertFalse($queryTags->isEmpty());
        $this->assertCount(3, $queryTags->toArray());
        $this->assertSame('test-run-123', $queryTags->toArray()['keboola_run_id']);
        $this->assertSame('test-branch-456', $queryTags->toArray()['keboola_branch_id']);
        $this->assertSame('sapi', $queryTags->toArray()['keboola_service']);
    }

    public function testAllTagsCombined(): void
    {
        $queryTags = new QueryTags([
            QueryTagKey::BRANCH_ID->value => 'branch-1',
            QueryTagKey::RUN_ID->value => 'run-1',
            QueryTagKey::KEBOOLA_RUN_ID->value => 'run-1',
            QueryTagKey::KEBOOLA_BRANCH_ID->value => 'branch-1',
            QueryTagKey::KEBOOLA_SERVICE->value => 'sapi',
        ]);

        $this->assertFalse($queryTags->isEmpty());
        $this->assertCount(5, $queryTags->toArray());
    }

    #[DataProvider('validQueryTagsValuesProvider')]
    public function testValidQueryTagsValues(string $value): void
    {
        $queryTags = new QueryTags();
        $queryTags->addTag(QueryTagKey::BRANCH_ID->value, $value);
        $this->assertEquals($value, $queryTags->toArray()[QueryTagKey::BRANCH_ID->value]);
    }

    /**
     * @return array<string, array{string}>
     */
    public static function validQueryTagsValuesProvider(): array
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

    #[DataProvider('invalidQueryTagsValuesProvider')]
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
    public static function invalidQueryTagsValuesProvider(): array
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
