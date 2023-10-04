<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens;

class InternalToken
{
    public function __construct(
        public readonly string $type,
        public readonly string $token,
    ) {
    }
}
