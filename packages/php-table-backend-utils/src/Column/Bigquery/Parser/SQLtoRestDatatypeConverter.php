<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser;

use ArrayIterator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\Datatype\Definition\Exception\InvalidLengthException;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\TokenizerNestedToken;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\TokenizerToken;

/**
 * @phpstan-import-type BigqueryTableFieldSchema from Bigquery
 *
 * Class can convert BigqueryColumn defition to Bigquery REST API schema
 * REST API schema is defined by custom type Bigquery::BigqueryTableFieldSchema
 */
final class SQLtoRestDatatypeConverter
{
    /**
     * @phpstan-param array{type: string,name:string}|BigqueryTableFieldSchema $schema
     * @phpstan-return BigqueryTableFieldSchema
     *
     * Function will set REST api format for length of column
     * - maxLength
     * - precision
     * - scale
     */
    private static function updateSchemaLength(?string $length, array $schema): array
    {
        if ($length === null) {
            return $schema;
        }
        switch ($schema['type']) {
            case Bigquery::TYPE_STRING:
            case Bigquery::TYPE_BYTES:
                $schema['maxLength'] = $length;
                break;
            case Bigquery::TYPE_NUMERIC:
            case Bigquery::TYPE_BIGNUMERIC:
                $lengthItems = explode(',', $length, 2);
                if (count($lengthItems) === 2) {
                    $schema['precision'] = $lengthItems[0];
                    $schema['scale'] = $lengthItems[1];
                } elseif (count($lengthItems) === 1) {
                    $schema['precision'] = $lengthItems[0];
                }
                break;
        }
        return $schema;
    }

    /**
     * @phpstan-param array{name?:string}|BigqueryTableFieldSchema $schema
     * @phpstan-param ArrayIterator<int, TokenizerToken|TokenizerNestedToken> $tokens
     * @phpstan-return array<mixed>|BigqueryTableFieldSchema
     *
     * Recursive funtion to convert tokenized SQL definition to REST API schema
     */
    private static function convertTokenToSchema(
        TokenizerNestedToken|TokenizerToken $token,
        array $schema,
        ArrayIterator $tokens
    ): array {
        if ($token->type === ComplexTypeTokenizer::T_NAME) {
            // handle T_NAME name of column
            $schema['name'] = $token->token;
            // set point to next which is always type after name
            $tokens->next();
            /** @var TokenizerToken $typeToken */
            $typeToken = $tokens->current();
            if ($typeToken->type === ComplexTypeTokenizer::T_TYPE) {
                $schema = self::convertTokenToSchema($typeToken, $schema, $tokens);
            } else {
                throw new ParsingComplexTypeLengthException(sprintf(
                    'Unexpected token "%s" for field "%s". Type of field is expected.',
                    $typeToken->token,
                    $token->token,
                ));
            }
            return $schema;
        }

        if ($token->type === ComplexTypeTokenizer::T_TYPE) {
            // handle T_TYPE datatype of column
            $schema['type'] = $token->token;
            if ($token->token === 'ARRAY') {
                // REPEATED can be RECORD or TYPE
                $schema['mode'] = 'REPEATED';
                $tokens->next();
                // set pointer to next token which is always NESTED `ARRAY<TYPE>`
                /** @var TokenizerToken|TokenizerNestedToken $arrayTokenNested */
                $arrayTokenNested = $tokens->current();
                assert(
                    $arrayTokenNested instanceof TokenizerNestedToken,
                    sprintf('Expected class "%s" got "%s"', TokenizerNestedToken::class, $arrayTokenNested::class)
                );
                $nestedTokens = $arrayTokenNested->nested;
                $firstNestedToken = $nestedTokens->current();
                $schema = self::convertTokenToSchema($firstNestedToken, $schema, $nestedTokens);
            } elseif ($token->token === 'STRUCT') {
                // RECORD(STRUCT) but not repeated ARRAY
                $schema['type'] = 'RECORD';
                $schema['fields'] = [];
                // set pointer to next token which is always NESTED for STRUCT
                $tokens->next();
                /** @var TokenizerToken|TokenizerNestedToken $structNested */
                $structNested = $tokens->current();
                assert(
                    $structNested instanceof TokenizerNestedToken,
                    sprintf('Expected class "%s" got "%s"', TokenizerNestedToken::class, $structNested::class)
                );
                foreach ($structNested->nested as $nestedToken) {
                    if ($nestedToken->type !== ComplexTypeTokenizer::T_NAME) {
                        // each STRUCT field must start with T_NAME
                        continue;
                    }
                    $schema['fields'][] = self::convertTokenToSchema($nestedToken, [], $structNested->nested);
                }
            } else {
                // other types than complex STRUCT or ARRAY
                $tokens->next();
                if ($tokens->valid()) {
                    /** @var TokenizerToken $lengthToken */
                    $lengthToken = $tokens->current();
                    if ($lengthToken->type === ComplexTypeTokenizer::T_LENGTH) {
                        // if next token is T_LENGTH then update schema length
                        /** @phpstan-ignore-next-line */
                        $schema = self::updateSchemaLength($lengthToken->token, $schema);
                    } elseif ($lengthToken->type === ComplexTypeTokenizer::T_FIELD_DELIMITER) {
                        // if next token is `,` T_FIELD_DELIMITER then end to start another loop with next field
                        return $schema;
                    } else {
                        throw new ParsingComplexTypeLengthException(sprintf(
                            'Unexpected token "%s" for column "%s" of type "%s". Length of column is expected.',
                            $lengthToken->token,
                            $schema['name'] ?? 'unknown',
                            $schema['type'],
                        ));
                    }
                }
            }
            return $schema;
        }
        if ($token->type === ComplexTypeTokenizer::T_FIELD_DELIMITER) {
            // if next token is `,` T_FIELD_DELIMITER then end to start another loop with next field
            return $schema;
        }
        throw new ParsingComplexTypeLengthException(sprintf(
            'Unexpected token "%s" for field "%s". Name or type of field is expected.',
            $token->type === ComplexTypeTokenizer::T_NESTED ? '<' : $token->token,
            $schema['name'] ?? 'unknown',
        ));
    }

    /**
     * @phpstan-return BigqueryTableFieldSchema
     */
    public static function convertColumnToRestFormat(BigqueryColumn $column): array
    {
        $definition = $column->getColumnDefinition();
        if (in_array(strtoupper($definition->getType()), [Bigquery::TYPE_ARRAY, Bigquery::TYPE_STRUCT], true)) {
            if ($definition->getLength() === null | $definition->getLength() === '') {
                throw new InvalidLengthException(
                    sprintf(
                        'Invalid column "%s" definition "%s". STRUCT|ARRAY type must have definition.',
                        $column->getColumnName(),
                        $definition->getSQLDefinition(),
                    ),
                );
            }
            // complex types
            try {
                $tokens = (new ComplexTypeTokenizer())->tokenize(
                    $column->getColumnName() . ' ' . $definition->getSQLDefinition()
                );
                $schema = self::convertTokenToSchema($tokens->current(), [], $tokens);
            } catch (ParsingComplexTypeLengthException $e) {
                throw new InvalidLengthException(
                    sprintf(
                        'Invalid column "%s" definition "%s". %s',
                        $column->getColumnName(),
                        $definition->getSQLDefinition(),
                        $e->getMessage(),
                    ),
                );
            }
        } else {
            // others
            $schema = [
                'name' => $column->getColumnName(),
                'type' => $definition->getType(),
            ];
            $schema = self::updateSchemaLength($definition->getLength(), $schema);
            if ($definition->isNullable() === false) {
                $schema['mode'] = 'REQUIRED';
            }
            if ($definition->getDefault() !== null && $definition->getDefault() !== '') {
                $schema['defaultValueExpression'] = $definition->getDefault();
            }
        }
        /** @phpstan-var BigqueryTableFieldSchema */
        return $schema;
    }
}
