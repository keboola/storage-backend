<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens;

class TokenizerTokenWithLevel extends TokenizerToken
{
    public function __construct(
        string $type,
        string $token,
        public readonly int $level,
    ) {
        parent::__construct($type, $token);
    }
}
