<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Escaping;

interface QuoteInterface
{
    public static function quote(string $value): string;

    public static function quoteSingleIdentifier(string $str): string;
}
