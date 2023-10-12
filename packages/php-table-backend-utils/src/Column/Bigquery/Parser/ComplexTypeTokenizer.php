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
    private const REGEX_MATCH_TYPE = <<<EOD
/ # Matches types ARRAY<> or types inside complex types INT, INT() ,...
(\w+)  # Match and capture one or more word characters
# Match zero or more occurrences of the following non-capturing group
(
  # Start of non-capturing group
  (?:  # Match the following, but don't capture it
    <  # Match the opening angle bracket
    .* # Match any character (.) zero or more times (*) inside brackets
    >  # Match the closing angle bracket
  )             
  *  # Match the non-capturing group zero or more times
)
/ix
EOD;
    private const REGEX_MATCH_NAME = <<<EOD
/ # Name requires need space after it there is a space or end of string
^                # Start at the beginning of the string
[a-zA-Z0-9-_]+   # Match one or more alphanumeric characters, hyphens, or underscores
(?=\s|$)         # check for a space (\s) or the end of the string ($), but don't include it in the match
/ix
EOD;
    private const REGEX_MATCH_LENGTH = <<<EOD
/ # matches everything between () to get length of field
\(    # Match an opening parenthesis "("
(.*?) # Match and capture any characters (.) non-greedily (*) within a pair of parentheses
\)    # Match a closing parenthesis ")"
/x
EOD;

    /**
     * @phpstan-return ArrayIterator<int, TokenizerToken|TokenizerNestedToken>
     */
    public function tokenize(string $input): ArrayIterator
    {
        $tokens = new ArrayIterator([]);
        $index = 0;
        $length = strlen($input);

        $complexNestedLevel = 0;
        $expectTypeNext = false;
        $expectDelimiterOrCloseNext = false;
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
                $expectDelimiterOrCloseNext = false;
                $index++;
                continue;
            }

            if ($input[$index] === '<') {
                // T_COMPLEX_START
                $expectDelimiterOrCloseNext = false;
                $tokens->append(new TokenizerTokenWithLevel(self::T_COMPLEX_START, '<', $complexNestedLevel));
                $complexNestedLevel++;
                $index++;
                continue;
            }

            if ($input[$index] === '>') {
                // T_COMPLEX_END
                $complexNestedLevel--;
                $expectDelimiterOrCloseNext = false;
                $tokens->append(new TokenizerTokenWithLevel(self::T_COMPLEX_END, '>', $complexNestedLevel));
                $index++;
                continue;
            }

            if (preg_match(self::REGEX_MATCH_NAME, substr($input, $index), $matchName, PREG_NO_ERROR)) {
                $positionCharacterAfter = $index + strlen($matchName[0]);
                if ($length > $positionCharacterAfter && $input[$positionCharacterAfter] === ' ') {
                    if ($expectTypeNext) {
                        throw new ParsingComplexTypeLengthException(sprintf(
                            'Unexpected token on position "%d" in "%s". Type of field expected.',
                            $index,
                            substr($input, $index)
                        ));
                    }
                    $expectTypeNext = true;
                    // if next character is space, then this is T_NAME (name of column)
                    $tokens->append(new TokenizerToken(self::T_NAME, $matchName[0]));
                    $index += strlen($matchName[0]);
                    continue;
                }
            }

            if (preg_match(self::REGEX_MATCH_TYPE, substr($input, $index), $matchType, PREG_NO_ERROR)) {
                if ($expectDelimiterOrCloseNext) {
                    throw new ParsingComplexTypeLengthException(sprintf(
                        // phpcs:ignore
                        'Unexpected token on position "%d" in "%s". Expected "," followed by next field or end of ARRAY|STRUCT.',
                        $index,
                        substr($input, $index)
                    ));
                }
                $expectTypeNext = false;
                $expectDelimiterOrCloseNext = true;
                $tokens->append(new TokenizerToken(self::T_TYPE, $matchType[1]));
                $index += strlen($matchType[1]);
                // check type followed by length definition
                if ($length > $index && $input[$index] === '(') {
                    // length start
                    if (preg_match(self::REGEX_MATCH_LENGTH, substr($input, $index), $matchLength)) {
                        $tokens->append(new TokenizerToken(self::T_LENGTH, $matchLength[1]));
                        $index += strlen($matchLength[1]) + 2; // skip content + 2 ()
                    } else {
                        throw new ParsingComplexTypeLengthException(sprintf(
                            'Unexpected token on position "%d" in "%s". Closing parenthesis not found.',
                            $index,
                            substr($input, $index)
                        ));
                    }
                }
                continue;
            }

            throw new ParsingComplexTypeLengthException(sprintf(
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
