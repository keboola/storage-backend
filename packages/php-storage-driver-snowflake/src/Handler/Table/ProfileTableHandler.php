<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Handler\Table;

use Google\Protobuf\Internal\Message;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse\Column;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Shared\Driver\BaseHandler;
use Keboola\StorageDriver\Shared\Utils\ProtobufHelper;
use Keboola\StorageDriver\Snowflake\ConnectionFactory;
use Keboola\StorageDriver\Snowflake\Profile\Column\AvgMinMaxLengthColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\DistinctCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\DuplicateCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NullCountColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\Column\NumericStatisticsColumnMetric;
use Keboola\StorageDriver\Snowflake\Profile\ColumnMetricInterface;
use Keboola\StorageDriver\Snowflake\Profile\Table\ColumnCountTableMetric;
use Keboola\StorageDriver\Snowflake\Profile\Table\DataSizeTableMetric;
use Keboola\StorageDriver\Snowflake\Profile\Table\RowCountTableMetric;
use Keboola\StorageDriver\Snowflake\Profile\TableMetricInterface;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableReflection;

final class ProfileTableHandler extends BaseHandler
{
    /**
     * @inheritDoc
     * @param GenericBackendCredentials $credentials
     * @param CreateProfileTableCommand $command
     */
    public function __invoke(
        Message $credentials,
        Message $command,
        array $features,
        Message $runtimeOptions,
    ): Message|null {
        assert($credentials instanceof GenericBackendCredentials);
        assert($command instanceof CreateProfileTableCommand);

        // Validate
        assert($command->getPath()->count() === 1, 'CreateProfileTableCommand.path is required and size must equal 1');
        assert($command->getTableName() !== '', 'CreateProfileTableCommand.tableName is required');

        $connection = ConnectionFactory::createFromCredentials($credentials);
        $schemaName = ProtobufHelper::repeatedStringToArray($command->getPath())[0];
        $tableName = $command->getTableName();

        $response = (new CreateProfileTableResponse())
            ->setPath($command->getPath())
            ->setTableName($tableName);

        /** @var TableMetricInterface[] $tableMetrics */
        $tableMetrics = [
            new RowCountTableMetric(),
            new ColumnCountTableMetric(),
            new DataSizeTableMetric(),
        ];

        $tableProfile = [];
        foreach ($tableMetrics as $metric) {
            $tableProfile[$metric->name()] = $metric->collect(
                $schemaName,
                $tableName,
                $connection,
            );
        }

        $response->setProfile(json_encode($tableProfile, JSON_THROW_ON_ERROR));
        $tableReflection = new SnowflakeTableReflection($connection, $schemaName, $tableName);

        $columnProfiles = [];
        foreach ($tableReflection->getColumnsDefinitions() as $column) {
            $columnName = $column->getColumnName();
            $columnMetrics = $this->columnMetricsByType($column->getColumnDefinition()->getType());

            $columnProfile = [];
            foreach ($columnMetrics as $metric) {
                $columnProfile[$metric->name()] = $metric->collect(
                    $schemaName,
                    $tableName,
                    $columnName,
                    $connection,
                );
            }

            $columnProfiles[] = (new Column())
                ->setName($columnName)
                ->setProfile(json_encode($columnProfile, JSON_THROW_ON_ERROR));
        }

        $response->setColumns($columnProfiles);

        return $response;
    }

    /**
     * @return ColumnMetricInterface[]
     */
    private function columnMetricsByType(string $type): array
    {
        $default = [
            new DistinctCountColumnMetric(),
            new DuplicateCountColumnMetric(),
            new NullCountColumnMetric(),
        ];

        $extra = match ($type) {
            Snowflake::TYPE_FLOAT,
            Snowflake::TYPE_NUMBER => [
                new NumericStatisticsColumnMetric(),
            ],
            Snowflake::TYPE_VARCHAR => [
                new AvgMinMaxLengthColumnMetric(),
            ],
            default => [],
        };

        return array_merge($default, $extra);
    }
}
