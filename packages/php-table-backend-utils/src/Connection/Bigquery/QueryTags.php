<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use InvalidArgumentException;

/**
 * Class for managing and validating BigQuery query labels
 */
class QueryTags
{
    private const MAX_LENGTH = 63;
    // Pattern allows lowercase letters (including international), numbers, underscores, and dashes
    private const VALUE_PATTERN = '/^[\p{Ll}0-9_-]*$/u';

    /** @var array<string, string> */
    private array $queryTags = [];

    /**
     * @param array<string, string> $tags
     */
    public function __construct(array $tags = [])
    {
        foreach ($tags as $key => $value) {
            $this->addTag($key, $value);
        }
    }

    /** @throws InvalidArgumentException if the value is invalid */
    public function addTag(string $key, string $value): self
    {
        // Validate the key using QueryTagKey enum
        QueryTagKey::validateKey($key);

        // Validate the value format
        $this->validateQueryTagValue($key, $value);

        $this->queryTags[$key] = $value;
        return $this;
    }

    /**
     * Validates if the given value follows BigQuery query label requirements:
     * - Can be no longer than 63 characters
     * - Can only contain lowercase letters (including international), numeric characters, underscores and dashes
     * - Empty values are allowed
     *
     * @throws InvalidArgumentException if the value is invalid
     */
    private function validateQueryTagValue(string $key, string $value): void
    {
        if (strlen($value) > self::MAX_LENGTH) {
            throw new InvalidArgumentException(sprintf(
                'Query tag value "%s" for key "%s" is too long. Maximum length is %d characters.',
                $value,
                $key,
                self::MAX_LENGTH,
            ));
        }

        if ($value !== '' && !preg_match(self::VALUE_PATTERN, $value)) {
            throw new InvalidArgumentException(sprintf(
                'Invalid query tag value "%s" for key "%s". Values can only contain' .
                ' lowercase letters (including international characters), numbers, underscores and dashes.',
                $value,
                $key,
            ));
        }
    }

    public function isEmpty(): bool
    {
        return empty($this->queryTags);
    }

    /**
     * @return array<string, string>
     */
    public function toArray(): array
    {
        return $this->queryTags;
    }
}
