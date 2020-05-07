<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils;

use Keboola\CommonExceptions\ApplicationExceptionInterface;
use RuntimeException;

class QueryBuilderException extends RuntimeException implements ApplicationExceptionInterface
{
    public const STRING_CODE_INVALID_TEMP_TABLE = 'invalidTempTable';
    public const STRING_CODE_NOT_ALLOWED_GRANT = 'grantNotAllowed';

    /**
     * @var string
     */
    private $stringCode;

    public function __construct(
        string $message,
        string $stringCode,
        ?\Throwable $previous = null
    ) {
        parent::__construct($message, 0, $previous);
        $this->stringCode = $stringCode;
    }

    public function getStringCode(): string
    {
        return $this->stringCode;
    }
}
