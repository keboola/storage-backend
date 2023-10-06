<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser;

use ArrayIterator;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\TokenizerNestedToken;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\TokenizerToken;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\TokenizerTokenWithLevel;
use RuntimeException;
use const PREG_NO_ERROR;

/**
 * Class to tokenize SQL complex types definition in BigQuery
 * can parse sql like:
 * - columnName ARRAY<definition>
 * - columnName STRUCT<definition>
 * even complex nested structures available in BQ
 */
final class ComplexTypeTokenizer
{
    // name of field
    public const T_NAME = 'T_NAME';
    // sql type
    public const T_TYPE = 'T_TYPE';
    // length of simple types inside struct and array
    public const T_LENGTH = 'T_LENGTH';
    // comma `,` delimiter between fields of struct
    public const T_FIELD_DELIMITER = 'T_FIELD_DELIMITER';
    // nested content of struct or array
    public const T_NESTED = 'T_NESTED';
    // start of complex type <
    private const T_COMPLEX_START = 'T_COMPLEX_START';
    // end of complex type >
    private const T_COMPLEX_END = 'T_COMPLEX_END';

    /**
     * @phpstan-return ArrayIterator<int, TokenizerToken|TokenizerNestedToken>
     */
    public function tokenize(string $input): ArrayIterator
    {
        $tokens = new ArrayIterator([]);
        $index = 0;
        $length = strlen($input);

        $complexNestedLevel = 0;
        while ($index < $length) {
            if ($input[$index] === ' ') {
                // skip whitespace
                // whitespaces are only between T_NAME and T_TYPE
                $index++;
                continue;
            }

            if ($input[$index] === ',') {
                // comma is only between fields in struct
                $tokens->append(new TokenizerToken(self::T_FIELD_DELIMITER, ','));
                $index++;
                continue;
            }

            if ($input[$index] === '<') {
                // T_COMPLEX_START
                $tokens->append(new TokenizerTokenWithLevel(self::T_COMPLEX_START, '<', $complexNestedLevel));
                $complexNestedLevel++;
                $index++;
                continue;
            }

            if ($input[$index] === '>') {
                // T_COMPLEX_END
                $complexNestedLevel--;
                $tokens->append(new TokenizerTokenWithLevel(self::T_COMPLEX_END, '>', $complexNestedLevel));
                $index++;
                continue;
            }

            if (preg_match('/^[a-zA-Z0-9-_]+(?=\s|$)/i', substr($input, $index), $matchName, PREG_NO_ERROR)) {
                $positionCharacterAfter = $index + strlen($matchName[0]);
                if ($length > $positionCharacterAfter && $input[$positionCharacterAfter] === ' ') {
                    // if next character is space, then this is T_NAME (name of column)
                    $tokens->append(new TokenizerToken(self::T_NAME, $matchName[0]));
                    $index += strlen($matchName[0]);
                    continue;
                }
            }

            if (preg_match('/(\w+)(?:<.*>)*/', substr($input, $index), $matchType, PREG_NO_ERROR)) {
                $tokens->append(new TokenizerToken(self::T_TYPE, $matchType[1]));
                $index += strlen($matchType[1]);
                // check type followed by length definition
                if ($length > $index && $input[$index] === '(') {
                    // length start
                    if (preg_match('/\((.*?)\)/', substr($input, $index), $matchLength)) {
                        $tokens->append(new TokenizerToken(self::T_LENGTH, $matchLength[1]));
                        $index += strlen($matchLength[1]) + 2; // skip content + 2 ()
                    } else {
                        throw new RuntimeException(sprintf(
                            'Unexpected token on position "%d" in "%s". Closing parenthesis not found.',
                            $index,
                            substr($input, $index)
                        ));
                    }
                }
                continue;
            }

            throw new RuntimeException(sprintf(
                'Unexpected token "%s" on position "%d" in "%s"',
                $input[$index],
                $index,
                substr($input, $index)
            ));
        }

        return $this->createNestedTree($tokens);
    }

    /**
     * @phpstan-param ArrayIterator<int, TokenizerToken|TokenizerTokenWithLevel> $tokens
     * @phpstan-return ArrayIterator<int, TokenizerToken|TokenizerNestedToken>
     */
    private function createNestedTree(ArrayIterator $tokens): ArrayIterator
    {
        $result = new ArrayIterator([]);
        /** @var TokenizerToken|TokenizerTokenWithLevel $token */
        foreach ($tokens as $token) {
            if ($token->type === self::T_COMPLEX_START) {
                $nestedTokens = new ArrayIterator([]);
                assert($token instanceof TokenizerTokenWithLevel);
                $level = $token->level;
                $tokens->next(); // skip T_COMPLEX_START
                while ($tokens->valid()) {
                    $current = $tokens->current();
                    if ($current->type === self::T_COMPLEX_END) {
                        assert($current instanceof TokenizerTokenWithLevel);
                        if ($current->level === $level) {
                            break; // end of current struct level
                        }
                    }
                    $nestedTokens->append($current);
                    $tokens->next();
                }
                $result->append(new TokenizerNestedToken($this->createNestedTree($nestedTokens)));
            } else {
                $result->append(new TokenizerToken($token->type, $token->token));
            }
        }

        return $result;
    }
}
