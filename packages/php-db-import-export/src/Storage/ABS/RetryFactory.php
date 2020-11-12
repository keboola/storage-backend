<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Storage\ABS;

use MicrosoftAzure\Storage\Common\Internal\Resources;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddleware;
use MicrosoftAzure\Storage\Common\Middlewares\RetryMiddlewareFactory;

final class RetryFactory
{
    public static function createRetryMiddleware(): RetryMiddleware
    {
        return RetryMiddlewareFactory::create(
            RetryMiddlewareFactory::GENERAL_RETRY_TYPE,
            Resources::DEFAULT_NUMBER_OF_RETRIES,
            Resources::DEFAULT_RETRY_INTERVAL,
            RetryMiddlewareFactory::EXPONENTIAL_INTERVAL_ACCUMULATION,
            true
        );
    }
}
