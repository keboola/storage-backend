<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

class DataHelper
{
    /**
     * @param array<mixed> $data
     * @param string $key
     * @return array<mixed>
     */
    public static function extractByKey(array $data, string $key): array
    {
        return array_map(static function ($record) use ($key) {
            return trim($record[$key]);
        }, $data);
    }
}
