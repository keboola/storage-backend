<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens;

use ArrayIterator;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\ComplexTypeTokenizer;

class TokenizerNestedToken extends TokenizerToken
{
    /**
     * @param ArrayIterator<int, TokenizerToken|TokenizerNestedToken> $nested
     */
    public function __construct(
        public readonly ArrayIterator $nested,
    ) {
        parent::__construct(ComplexTypeTokenizer::T_NESTED, 'nested');
    }
}
