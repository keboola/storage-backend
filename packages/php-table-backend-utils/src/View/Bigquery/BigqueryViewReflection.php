<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\View\Bigquery;

use Google\Cloud\BigQuery\BigQueryClient;
use Google\Cloud\BigQuery\Exception\JobException;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;
use Keboola\TableBackendUtils\Table\Bigquery\BigqueryTableReflection;
use Keboola\TableBackendUtils\View\InvalidViewDefinitionException;
use Keboola\TableBackendUtils\View\ViewReflectionInterface;

class BigqueryViewReflection implements ViewReflectionInterface
{
    private BigQueryClient $bqClient;

    private string $datasetName;

    private string $viewName;

    private bool $isTemporary = false;

    public function __construct(BigQueryClient $bqClient, string $datasetName, string $tableName)
    {
        $this->viewName = $tableName;
        $this->datasetName = $datasetName;
        $this->bqClient = $bqClient;
    }

    public function getDependentViews(): array
    {
        return BigqueryTableReflection::getDependentViewsForObject(
            $this->bqClient,
            $this->viewName,
            $this->datasetName,
            BigqueryTableReflection::DEPENDENT_OBJECT_VIEW
        );
    }

    public function getViewDefinition(): string
    {
        $result = $this->bqClient->runQuery($this->bqClient->query(sprintf(
            'SELECT ddl as text FROM %s.INFORMATION_SCHEMA.TABLES WHERE table_name = %s AND table_type = \'VIEW\'',
            BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            BigqueryQuote::quote($this->viewName)
        )));

        return $result->getIterator()->current() ? $result->getIterator()->current()['text'] : '';
    }

    public function refreshView()
    {
        $definition = $this->getViewDefinition();

        $objectNameWithSchema = sprintf(
            '%s.%s',
            BigqueryQuote::quoteSingleIdentifier($this->datasetName),
            BigqueryQuote::quoteSingleIdentifier($this->viewName)
        );

        $this->bqClient->runQuery($this->bqClient->query(sprintf('DROP VIEW %s', $objectNameWithSchema)));
        try {
            $this->bqClient->runQuery(
                $this->bqClient->query(
                    sprintf(
                    'CREATE VIEW %s.%s AS %s',
                    BigqueryQuote::quoteSingleIdentifier($this->datasetName),
                    BigqueryQuote::quoteSingleIdentifier($this->viewName),
                    $definition
                    )
                )
            );
        } catch (JobException $e) {
            throw InvalidViewDefinitionException::createViewRefreshError($this->datasetName, $this->viewName, $e);
        }
    }
}
