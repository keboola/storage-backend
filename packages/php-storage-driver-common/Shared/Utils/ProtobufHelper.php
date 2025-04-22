<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Shared\Utils;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\RepeatedField;

class ProtobufHelper
{
    /**
     * Convert RepeatedField to Array: https://github.com/protocolbuffers/protobuf/issues/7648
     * @param RepeatedField<string> $repeated
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

    /**
     * Convert array to RepeatedField
     *
     * @param string[] $repeated
     * @return RepeatedField<string>
     */
    public static function arrayToRepeatedString(array $repeated): RepeatedField
    {
        $out = new RepeatedField(GPBType::STRING);
        foreach ($repeated as $value) {
            $out[] = $value;
        }
        return $out;
    }
}
