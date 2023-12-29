<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Connection\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;

class SessionFactory
{
    private BigQueryClient $bqClient;

    public function __construct(BigQueryClient $bqClient)
    {
        $this->bqClient = $bqClient;
    }

    public function createSession(): Session
    {
        return Session::createFromJob($this->bqClient->runJob(
            $this->bqClient->query(
                'SELECT 1',
                [
                    'configuration' => [
                        'query' => [
                            'createSession' => true,
                        ],
                    ],
                ],
            ),
        ));
    }
}
