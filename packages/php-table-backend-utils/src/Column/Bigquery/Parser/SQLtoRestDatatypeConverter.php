<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Column\Bigquery\Parser;

use ArrayIterator;
use Keboola\Datatype\Definition\Bigquery;
use Keboola\TableBackendUtils\Column\Bigquery\BigqueryColumn;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\InternalToken;
use Keboola\TableBackendUtils\Column\Bigquery\Parser\Tokens\InternalTokenWithNested;
use RuntimeException;

/**
 * @phpstan-import-type BigqueryTableFieldSchema from Bigquery
 */
final class SQLtoRestDatatypeConverter
{
    /**
     * @phpstan-param array{type: string,name:string}|BigqueryTableFieldSchema $schema
     * @phpstan-return BigqueryTableFieldSchema
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
     * @phpstan-param ArrayIterator<int, InternalToken|InternalTokenWithNested> $tokens
     * @phpstan-return array<mixed>|BigqueryTableFieldSchema
     */
    private static function convertTokenToSchema(
        InternalTokenWithNested|InternalToken $token,
        array $schema,
        ArrayIterator $tokens
    ): array {
        if ($token->type === ComplexTypeTokenizer::T_NAME) {
            $schema['name'] = $token->token;
            // set point to next which is always type after name
            $tokens->next();
            /** @var InternalToken $typeToken */
            $typeToken = $tokens->current();
            if ($typeToken->type === ComplexTypeTokenizer::T_TYPE) {
                $schema = self::convertTokenToSchema($typeToken, $schema, $tokens);
            } else {
                throw new RuntimeException(sprintf(
                    'Unexpected token "%s" for field "%s". Type of field is expected.',
                    $typeToken->token,
                    $token->token,
                ));
            }
            return $schema;
        }

        if ($token->type === ComplexTypeTokenizer::T_TYPE) {
            $schema['type'] = $token->token;
            if ($token->token === 'ARRAY') {
                // REPEATED can be RECORD or TYPE
                $schema['mode'] = 'REPEATED';
                $tokens->next();
                // set pointer to next token which is always NESTED `ARRAY<TYPE>`
                /** @var InternalToken|InternalTokenWithNested $arrayTokenNested */
                $arrayTokenNested = $tokens->current();
                assert(
                    $arrayTokenNested instanceof InternalTokenWithNested,
                    sprintf('Expected class "%s" got "%s"', InternalTokenWithNested::class, $arrayTokenNested::class)
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
                /** @var InternalToken|InternalTokenWithNested $structNested */
                $structNested = $tokens->current();
                assert(
                    $structNested instanceof InternalTokenWithNested,
                    sprintf('Expected class "%s" got "%s"', InternalTokenWithNested::class, $structNested::class)
                );
                foreach ($structNested->nested as $nestedToken) {
                    if ($nestedToken->type !== ComplexTypeTokenizer::T_NAME) {
                        continue;
                    }
                    $schema['fields'][] = self::convertTokenToSchema($nestedToken, [], $structNested->nested);
                }
            } else {
                // other types than complex STRUCT or ARRAY
                $tokens->next();
                if ($tokens->valid()) {
                    /** @var InternalToken $lengthToken */
                    $lengthToken = $tokens->current();
                    if ($lengthToken->type === ComplexTypeTokenizer::T_LENGTH) {
                        /** @phpstan-ignore-next-line */
                        $schema = self::updateSchemaLength($lengthToken->token, $schema);
                    } elseif ($lengthToken->type === ComplexTypeTokenizer::T_FIELD_DELIMITER) {
                        return $schema;
                    } else {
                        throw new RuntimeException(sprintf(
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
            return $schema;
        }
        throw new RuntimeException(sprintf(
            'Unexpected token "%s" for field "%s". Name or type of field is expected.',
            $token->token,
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
            // complex types
            $tokens = (new ComplexTypeTokenizer())->tokenize(
                $column->getColumnName() . ' ' . $definition->getSQLDefinition()
            );
            $schema = self::convertTokenToSchema($tokens->current(), [], $tokens);
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
