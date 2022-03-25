<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

use Google\Protobuf\Internal\RepeatedField;

class ProtobufHelper
{
    /**
     * Convert RepeatedField to Array: https://github.com/protocolbuffers/protobuf/issues/7648
     *
     * @return string[]
     */
    public static function repeatedStringToArray(RepeatedField $repeated): array
    {
        $values = [];
        foreach ($repeated as $value) {
            $values[] = (string) $value;
        }
        return $values;
    }
}
