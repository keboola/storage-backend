<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

use Exception;

class DataHelper
{
    /**
     * @param array<mixed> $data
     * @param string $key
     * @return array<mixed>
     * @throws Exception
     */
    public static function extractByKey(array $data, string $key): array
    {
        return array_map(static function ($record) use ($key) {
            // throwing exception in order be able to catch it
            if (!array_key_exists($key, $record)) {
                throw new Exception(sprintf('Key %s is not defined in array', $key));
            }
            $val = $record[$key];
            // trimming because TD loves to return data with some extra spaces
            return is_string($val) ? trim($val) : $val;
        }, $data);
    }
}
