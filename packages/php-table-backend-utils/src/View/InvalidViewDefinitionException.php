<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use RuntimeException;
use Throwable;

class InvalidViewDefinitionException extends RuntimeException implements ApplicationExceptionInterface
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }

    public static function createForNotExistingView(
        string $schemaName,
        string $viewName
    ): InvalidViewDefinitionException {
        return new self(
            sprintf(
                'View "%s" in schema "%s" does not exists.',
                $viewName,
                $schemaName
            )
        );
    }

    public static function createForMissingDefinition(
        string $schemaName,
        string $viewName
    ): InvalidViewDefinitionException {
        return new self(
            sprintf(
                'Definition of view "%s" in schema "%s"cannot be obtained from Synapse or it\'s invalid.',
                $viewName,
                $schemaName
            )
        );
    }

    public static function createViewRefreshError(
        string $schemaName,
        string $viewName,
        Throwable $previous
    ): InvalidViewDefinitionException {
        return new self(
            sprintf(
                'View "%s" in schema "%s" has to be refreshed manually, since it\'s definition cannot be refreshed.',
                $viewName,
                $schemaName
            ),
            $previous
        );
    }
}
