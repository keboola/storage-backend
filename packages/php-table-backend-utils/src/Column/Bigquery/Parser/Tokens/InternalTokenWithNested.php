<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens;

use ArrayIterator;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\ComplexTypeTokenizer;

class InternalTokenWithNested
{
    public string $type = ComplexTypeTokenizer::T_NESTED;

    public string $token = 'nested';

    /**
     * @param ArrayIterator<int, InternalToken|InternalTokenWithNested> $nested
     */
    public function __construct(
        public readonly ArrayIterator $nested,
    ) {
    }
}
