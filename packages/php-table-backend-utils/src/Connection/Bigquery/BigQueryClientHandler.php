<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use GuzzleHttp\Client;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Retry\BackOff\UniformRandomBackOffPolicy;
use Retry\Policy\SimpleRetryPolicy;
use Retry\RetryProxy;

/**
 * Guzzle http handler for BigQuery client with retry policy
 */
class BigQueryClientHandler
{
    private Client $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function __invoke(RequestInterface $request): ResponseInterface
    {
        $retryPolicy = new SimpleRetryPolicy(5);
        $proxy = new RetryProxy($retryPolicy, new UniformRandomBackOffPolicy());

        /** @var ResponseInterface $result */
        $result = $proxy->call(function () use ($request) {
            return $this->client->send($request);
        });
        return $result;
    }
}
