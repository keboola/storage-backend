<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser;

use ArrayIterator;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\InternalToken;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\InternalTokenWithLevel;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\InternalTokenWithNested;
use RuntimeException;
use const PREG_NO_ERROR;

final class ComplexTypeTokenizer
{
    public const T_NAME = 'T_NAME';
    public const T_TYPE = 'T_TYPE';
    public const T_LENGTH = 'T_LENGTH';
    public const T_FIELD_DELIMITER = 'T_FIELD_DELIMITER';
    public const T_NESTED = 'T_NESTED';
    private const T_COMPLEX_START = 'T_COMPLEX_START';
    private const T_COMPLEX_END = 'T_COMPLEX_END';

    /**
     * @phpstan-return ArrayIterator<int, InternalToken|InternalTokenWithNested>
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
                $tokens->append(new InternalToken(self::T_FIELD_DELIMITER, ','));
                $index++;
                continue;
            }

            if ($input[$index] === '<') {
                // T_COMPLEX_START
                $tokens->append(new InternalTokenWithLevel(self::T_COMPLEX_START, '<', $complexNestedLevel));
                $complexNestedLevel++;
                $index++;
                continue;
            }

            if ($input[$index] === '>') {
                // T_COMPLEX_END
                $complexNestedLevel--;
                $tokens->append(new InternalTokenWithLevel(self::T_COMPLEX_END, '>', $complexNestedLevel));
                $index++;
                continue;
            }

            if (preg_match('/\w+/', $input, $matchType, PREG_NO_ERROR, $index)) {
                $positionCharacterAfter = $index + strlen($matchType[0]);
                if ($length > $positionCharacterAfter && $input[$positionCharacterAfter] === ' ') {
                    // if next character is space, then this is T_NAME (name of column)
                    $tokens->append(new InternalToken(self::T_NAME, $matchType[0]));
                    $index += strlen($matchType[0]);
                    continue;
                }

                $tokens->append(new InternalToken(self::T_TYPE, $matchType[0]));
                $index += strlen($matchType[0]);
                // check type followed by length definition
                if ($length > $index && $input[$index] === '(') {
                    // length start
                    if (preg_match('/\((.*?)\)/', substr($input, $index), $matchLength)) {
                        $tokens->append(new InternalToken(self::T_LENGTH, $matchLength[1]));
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
                'Unexpected token on position "%d" in "%s"',
                $index,
                substr($input, $index)
            ));
        }

        return $this->createNestedTree($tokens);
    }

    /**
     * @phpstan-param ArrayIterator<int, InternalToken|InternalTokenWithLevel> $tokens
     * @phpstan-return ArrayIterator<int, InternalToken|InternalTokenWithNested>
     */
    private function createNestedTree(ArrayIterator $tokens): ArrayIterator
    {
        $result = new ArrayIterator([]);
        /** @var InternalToken|InternalTokenWithLevel $token */
        foreach ($tokens as $token) {
            if ($token->type === self::T_COMPLEX_START) {
                $nestedTokens = new ArrayIterator([]);
                assert($token instanceof InternalTokenWithLevel);
                $level = $token->level;
                $tokens->next(); // skip T_COMPLEX_START
                while ($tokens->valid()) {
                    $current = $tokens->current();
                    if ($current->type === self::T_COMPLEX_END) {
                        assert($current instanceof InternalTokenWithLevel);
                        if ($current->level === $level) {
                            break; // end of current struct level
                        }
                    }
                    $nestedTokens->append($current);
                    $tokens->next();
                }
                $result->append(new InternalTokenWithNested($this->createNestedTree($nestedTokens)));
            } else {
                $result->append(new InternalToken($token->type, $token->token));
            }
        }

        return $result;
    }
}
