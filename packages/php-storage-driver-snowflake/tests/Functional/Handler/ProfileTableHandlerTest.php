<?php

declare(strict_types=1);

namespace Keboola\StorageDriver\Snowflake\Tests\Functional\Handler;

use Google\Protobuf\Any;
use Keboola\Datatype\Definition\Snowflake;
use Keboola\StorageDriver\Command\Common\RuntimeOptions;
use Keboola\StorageDriver\Command\Table\CreateProfileTableCommand;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse;
use Keboola\StorageDriver\Command\Table\CreateProfileTableResponse\Column;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials;
use Keboola\StorageDriver\Credentials\GenericBackendCredentials\SnowflakeCredentialsMeta;
use Keboola\StorageDriver\Snowflake\Handler\Table\ProfileTableHandler;
use Keboola\StorageDriver\Snowflake\Tests\Functional\BaseCase;
use Keboola\TableBackendUtils\Column\ColumnCollection;
use Keboola\TableBackendUtils\Column\Snowflake\SnowflakeColumn;
use Keboola\TableBackendUtils\Escaping\Snowflake\SnowflakeQuote;
use Keboola\TableBackendUtils\Table\Snowflake\SnowflakeTableQueryBuilder;

final class ProfileTableHandlerTest extends BaseCase
{
    private const TABLE_NAME = 'profile_table_test';

    public function testCreateProfile(): void
    {
        $credentials = $this->createCredentials();

        $handler = new ProfileTableHandler();

        $command = new CreateProfileTableCommand();
        $command->setPath([self::SCHEMA_NAME]);
        $command->setTableName(self::TABLE_NAME);

        $response = $handler(
            $credentials,
            $command,
            [],
            new RuntimeOptions(),
        );

        $this->assertInstanceOf(CreateProfileTableResponse::class, $response);

        $this->assertSame('profile_table_test', $response->getTableName());
        $this->assertSame([self::SCHEMA_NAME], iterator_to_array($response->getPath()));
        $this->assertSame('{"rowCount":8,"columnCount":7}', $response->getProfile());

        /** @var array<string, string> $expectedColumnProfiles */
        $expectedColumnProfiles = [
            'id' => '{"distinctCount":8,"duplicateCount":0,"nullCount":0}',
            'col_varchar' => '{"distinctCount":7,"duplicateCount":1,"nullCount":0}',
            'col_bool' => '{"distinctCount":2,"duplicateCount":5,"nullCount":1}',
            'col_number' => '{"distinctCount":5,"duplicateCount":2,"nullCount":1}',
            'col_decimal' => '{"distinctCount":6,"duplicateCount":1,"nullCount":1}',
            'col_float' => '{"distinctCount":5,"duplicateCount":2,"nullCount":1}',
            'col_date' => '{"distinctCount":6,"duplicateCount":1,"nullCount":1}',
        ];

        /** @var Column[] $columns */
        $columns = iterator_to_array($response->getColumns());
        foreach ($columns as $column) {
            $this->assertInstanceOf(Column::class, $column);
            $this->assertArrayHasKey($column->getName(), $expectedColumnProfiles);
            $this->assertSame($expectedColumnProfiles[$column->getName()], $column->getProfile());
        }
    }

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection->executeQuery(
            sprintf(
                'DROP TABLE IF EXISTS %s.%s;',
                SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
                SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
            ),
        );
        $this->connection->executeQuery(
            (new SnowflakeTableQueryBuilder())->getCreateTableCommand(
                self::SCHEMA_NAME,
                self::TABLE_NAME,
                new ColumnCollection([
                    new SnowflakeColumn(
                        'id',
                        new Snowflake(Snowflake::TYPE_NUMBER, ['nullable' => false]),
                    ),
                    new SnowflakeColumn(
                        'col_varchar',
                        new Snowflake(Snowflake::TYPE_VARCHAR),
                    ),
                    new SnowflakeColumn(
                        'col_bool',
                        new Snowflake(Snowflake::TYPE_BOOLEAN),
                    ),
                    new SnowflakeColumn(
                        'col_number',
                        new Snowflake(Snowflake::TYPE_NUMBER),
                    ),
                    new SnowflakeColumn(
                        'col_decimal',
                        new Snowflake(Snowflake::TYPE_DECIMAL),
                    ),
                    new SnowflakeColumn(
                        'col_float',
                        new Snowflake(Snowflake::TYPE_FLOAT),
                    ),
                    new SnowflakeColumn(
                        'col_date',
                        new Snowflake(Snowflake::TYPE_DATE),
                    ),
                ]),
            ),
        );

        $this->connection->executeQuery(sprintf(
            // phpcs:disable
            <<<'SQL'
                INSERT INTO %s.%s ("id", "col_varchar", "col_bool", "col_number", "col_decimal", "col_float", "col_date") VALUES
                (1, 'Bluetooth Headphones', TRUE, 120, 29.99, 4.5, DATE '2023-03-01'),
                (2, 'Bluetooth Headphones', TRUE, 120, 29.99, 4.5, DATE '2023-03-01'),
                (3, 'Smartphone X200', FALSE, 0, 499.00, 4.5, DATE '2022-11-15'),
                (4, 'Wireless Mouse', TRUE, NULL, 15.50, 3.9, DATE '2024-01-20'),
                (5, 'Mechanical Keyboard', TRUE, 200, 89.90, 4.1, NULL),
                (6, '4K OLED TV', NULL, 0, NULL, 4.9, DATE '2020-09-05'),
                (7, 'USB-C Hub', TRUE, 30, 22.49, NULL, DATE '2023-12-12'),
                (8, 'Ultrabook', FALSE, 60, 75.00, 2.4, DATE '2021-05-25')
                SQL,
            // phpcs:enable
            SnowflakeQuote::quoteSingleIdentifier(self::SCHEMA_NAME),
            SnowflakeQuote::quoteSingleIdentifier(self::TABLE_NAME),
        ));
    }

    private function createCredentials(): GenericBackendCredentials
    {
        $host = (string) getenv('SNOWFLAKE_HOST');
        $user = (string) getenv('SNOWFLAKE_USER');
        $password = (string) getenv('SNOWFLAKE_PASSWORD');
        $port = (string) getenv('SNOWFLAKE_PORT');
        $warehouse = (string) getenv('SNOWFLAKE_WAREHOUSE');
        $database = (string) getenv('SNOWFLAKE_DATABASE');

        $any = new Any();
        $any->pack(
            (new SnowflakeCredentialsMeta())
                ->setWarehouse($warehouse)
                ->setDatabase($database),
        );

        return (new GenericBackendCredentials())
            ->setHost($host)
            ->setPort($port)
            ->setPrincipal($user)
            ->setSecret($password)
            ->setMeta($any);
    }
}
