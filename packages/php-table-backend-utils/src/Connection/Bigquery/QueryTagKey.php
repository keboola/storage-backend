<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use InvalidArgumentException;

enum QueryTagKey: string
{
    case BRANCH_ID = 'branch_id';

    /**
     * Validates if the given string is a valid tag key
     * @throws InvalidArgumentException if the key is invalid
     */
    public static function validateKey(string $key): void
    {
        $validKeys = array_map(fn(self $case) => $case->value, self::cases());
        if (!in_array($key, $validKeys, true)) {
            throw new InvalidArgumentException(sprintf(
                'Invalid query tag key "%s". Valid keys are: %s',
                $key,
                implode(', ', $validKeys),
            ));
        }
    }
}
