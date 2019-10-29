<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\CommandBuilder;

use Keboola\Csv\CsvFile;
use Keboola\Db\ImportExport\File;
use Keboola\Db\ImportExport\ImportOptions;

class AbsBuilder
{
    private const SLICED_FILES_CHUNK_SIZE = 1000;
    private const TIMESTAMP_COLUMN_NAME = '_timestamp';

    public static function buildFileCopyCommands(
        ImportOptions $importOptions,
        File\Azure $file,
        string $stagingTableName
    ): array {
        $filesToImport = $file->getManifestEntries();
        $commands = [];
        foreach (array_chunk($filesToImport, self::SLICED_FILES_CHUNK_SIZE) as $entries) {
            $commands[] = sprintf(
                'COPY INTO %s.%s 
                FROM %s
                CREDENTIALS=(AZURE_SAS_TOKEN=\'%s\')
                FILE_FORMAT = (TYPE=CSV %s )
                FILES = (%s)',
                self::quoteIdentifier($importOptions->getSchema()),
                self::quoteIdentifier($stagingTableName),
                self::quote($file->getContainerUrl()),
                $file->getSasToken(),
                implode(' ', self::buildCsvCopyCommandOptions($importOptions, $file->getCsvFile())),
                implode(
                    ', ',
                    array_map(
                        function ($entry) use ($file) {
                            return self::quote(strtr($entry, [$file->getContainerUrl() => '']));
                        },
                        $entries
                    )
                )
            );
        }
        return $commands;
    }

    public static function buildInsertAllIntoTargetTableCommand(
        ImportOptions $importOptions,
        string $stagingTableName
    ): string {
        $columnsSetSqlSelect = implode(', ', array_map(function ($column) use ($importOptions) {
            if (in_array($column, $importOptions->getConvertEmptyValuesToNull())) {
                return sprintf(
                    'IFF(%s = \'\', NULL, %s)',
                    self::quoteIdentifier($column),
                    self::quoteIdentifier($column)
                );
            }

            return sprintf(
                "COALESCE(%s, '') AS %s",
                self::quoteIdentifier($column),
                self::quoteIdentifier($column)
            );
        }, $importOptions->getColumns()));

        if (in_array(self::TIMESTAMP_COLUMN_NAME, $importOptions->getColumns())
            || $importOptions->useTimestamp() === false
        ) {
            return sprintf(
                'INSERT INTO %s.%s (%s) (SELECT %s FROM %s.%s)',
                self::quoteIdentifier($importOptions->getSchema()),
                self::quoteIdentifier($importOptions->getTableName()),
                self::getImplodedColumnsString($importOptions->getColumns(), ', '),
                $columnsSetSqlSelect,
                self::quoteIdentifier($importOptions->getSchema()),
                self::quoteIdentifier($stagingTableName)
            );
        }

        return sprintf(
            'INSERT INTO %s.%s (%s, "%s") (SELECT %s, \'%s\' FROM %s.%s)',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($importOptions->getTableName()),
            self::getImplodedColumnsString($importOptions->getColumns(), ', '),
            self::TIMESTAMP_COLUMN_NAME,
            $columnsSetSqlSelect,
            self::getNowFormatted(),
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($stagingTableName)
        );
    }

    public static function buildCreateStagingTableCommand(string $schema, string $tableName, array $columns): string
    {
        $columnsSql = array_map(function ($column) {
            return sprintf('%s varchar', self::quoteIdentifier($column));
        }, $columns);
        return sprintf(
            'CREATE TEMPORARY TABLE %s.%s (%s)',
            self::quoteIdentifier($schema),
            self::quoteIdentifier($tableName),
            implode(', ', $columnsSql)
        );
    }

    public static function buildBeginTransactionCommand(): string
    {
        return 'BEGIN TRANSACTION';
    }

    public static function buildCommitTransactionCommand(): string
    {
        return 'COMMIT';
    }

    public static function buildDeleteOldItemsCommand(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        $deleteSql = sprintf(
            'DELETE FROM %s.%s."src"',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($stagingTableName)
        );
        $deleteSql .= sprintf(
            ' USING %s.%s AS "dest',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($importOptions->getTableName())
        );
        $deleteSql .= ' WHERE ' . self::buildPrimayKeyWhereConditions($primaryKeys);
        return $deleteSql;
    }

    public static function buildInsertFromStagingToTargetTableCommand(
        ImportOptions $importOptions,
        string $stagingTableName
    ): string {
        if ($importOptions->useTimestamp()) {
            $insColumns = array_merge($importOptions->getColumns(), [self::TIMESTAMP_COLUMN_NAME]);
        } else {
            $insColumns = $importOptions->getColumns();
        }

        $sql = sprintf(
            'INSERT INTO %s.%s (%s)',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($importOptions->getTableName()),
            self::getImplodedColumnsString($insColumns, ', ')
        );

        $columnsSetSql = [];

        foreach ($importOptions->getColumns() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull())) {
                $columnsSetSql[] = sprintf(
                    'IFF("src".%s = \'\', NULL, %s)',
                    self::quoteIdentifier($columnName),
                    self::quoteIdentifier($columnName)
                );
            } else {
                $columnsSetSql[] = sprintf(
                    'COALESCE("src".%s, \'\')',
                    self::quoteIdentifier($columnName)
                );
            }
        }

        $sql .= ' SELECT ' . implode(',', $columnsSetSql);
        if ($importOptions->useTimestamp()) {
            $sql .= ", '{self::getNowFormatted()}' "; //TODO: in previous implementation it was same as time of UPDATE
        }
        $sql .= sprintf(
            ' FROM %s.%s AS "src"',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($stagingTableName)
        );

        return $sql;
    }

    public static function buildUpdateWithPkCommand(
        ImportOptions $importOptions,
        string $stagingTableName,
        array $primaryKeys
    ): string {
        $updateSql = sprintf(
            'UPDATE %s.%s AS "dest" SET ',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($importOptions->getTableName())
        );
        $columnsSet = [];
        foreach ($importOptions->getColumns() as $columnName) {
            if (in_array($columnName, $importOptions->getConvertEmptyValuesToNull())) {
                $columnsSet[] = sprintf(
                    '%s = IFF("src".%s = \'\', NULL, "src".%s)',
                    self::quoteIdentifier($columnName),
                    self::quoteIdentifier($columnName),
                    self::quoteIdentifier($columnName)
                );
            } else {
                $columnsSet[] = sprintf(
                    '%s = COALESCE("src".%s, \'\')',
                    self::quoteIdentifier($columnName),
                    self::quoteIdentifier($columnName)
                );
            }
        }

        $updateSql .= implode(', ', $columnsSet);
        if ($importOptions->useTimestamp()) {
            $updateSql .= sprintf(
                ', %s = \'%s\'',
                self::quoteIdentifier(self::TIMESTAMP_COLUMN_NAME),
                self::getNowFormatted()
            );
        }
        $updateSql .= sprintf(
            ' FROM %s.%s AS "src" ',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($stagingTableName)
        );
        $updateSql .= ' WHERE ' . self::buildPrimayKeyWhereConditions($primaryKeys);

        $columnsComparsionSql = array_map(
            function ($columnName) {
                return sprintf(
                    'COALESCE(TO_VARCHAR("dest".%s, \'\') != COALESCE("src".%s, \'\')',
                    self::quoteIdentifier($columnName),
                    self::quoteIdentifier($columnName)
                );
            },
            $importOptions->getColumns()
        );

        $updateSql .= ' AND (' . implode(' OR ', $columnsComparsionSql) . ') ';
        return $updateSql;
    }

    public static function buildDedupCommand(
        ImportOptions $importOptions,
        array $primaryKeys,
        string $stagingTableName,
        string $tempTableName
    ): string {
        if (empty($primaryKeys)) {
            return '';
        }

        $pkSql = implode(',', array_map(function ($column) {
            return self::quoteIdentifier($column);
        }, $importOptions->getColumns()));

        $depudeSql = 'SELECT ';
        $depudeSql .= implode(',', array_map(function ($column) {
            return 'a.' . self::quoteIdentifier($column);
        }, $importOptions->getColumns()));

        $depudeSql .= sprintf(
            ' FROM (SELECT %s, ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) AS "_row_number_" FROM %s.%s)',
            self::getImplodedColumnsString($importOptions->getColumns(), ', '),
            $pkSql,
            $pkSql,
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($stagingTableName)
        );

        $depudeSql .= ' AS a WHERE a."_row_number_" = 1';

        return sprintf(
            'INSERT INTO %s.%s (%s) %s',
            self::quoteIdentifier($importOptions->getSchema()),
            self::quoteIdentifier($tempTableName),
            self::getImplodedColumnsString($importOptions->getColumns(), ', '),
            $depudeSql
        );
    }

    public static function buildRenameTableCommand(
        string $schema,
        string $stagingTableName,
        string $targetTable
    ): string {
        return sprintf(
            'ALTER TABLE %s.%s RENAME TO %s.%s',
            self::quoteIdentifier($schema),
            self::quoteIdentifier($stagingTableName),
            self::quoteIdentifier($schema),
            self::quoteIdentifier($targetTable)
        );
    }

    public static function buildDropCommand(string $schema, string $tableName): string
    {
        return sprintf(
            'DROP TABLE %s.%s',
            self::quoteIdentifier($schema),
            self::quoteIdentifier($tableName)
        );
    }

    public static function buildTruncateTableCommand(string $schema, string $tableName): string
    {
        return sprintf(
            'TRUNCATE %s.%s',
            self::quoteIdentifier($schema),
            self::quoteIdentifier($tableName)
        );
    }

    private static function buildPrimayKeyWhereConditions(array $primaryKeys): string
    {
        $pkWhereSql = [];
        foreach ($primaryKeys as $pkCollumn) {
            $pkWhereSql[] = sprintf(
                '"dest".%s = COALESCE("src".%s\'\')',
                self::quoteIdentifier($pkCollumn),
                self::quoteIdentifier($pkCollumn)
            );
        }
        return implode(' AND ', $pkWhereSql) . ' ';
    }

    private static function buildCsvCopyCommandOptions(ImportOptions $importOptions, CsvFile $csvFile): array
    {
        $options = [
            sprintf('FIELD_DELIMITER = %s', self::quote($csvFile->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('SKIP_HEADER = %d', $importOptions->getNumberOfIgnoredLines());
        }

        if ($csvFile->getEnclosure()) {
            $options[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', self::quote($csvFile->getEnclosure()));
            $options[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvFile->getEscapedBy()) {
            $options[] = sprintf('ESCAPED_UNENCLOSED_FIELD = %s', self::quote($csvFile->getEscapedBy()));
        }
        return $options;
    }

    private static function quote(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    public static function quoteIdentifier(string $value): string
    {
        $q = '"';
        return ($q . str_replace("$q", "$q$q", $value) . $q);
    }

    private static function getNowFormatted(): string
    {
        $currentDate = new \DateTime('now', new \DateTimeZone('UTC'));
        return $currentDate->format('Y-m-d H:i:s');
    }

    private static function getImplodedColumnsString(array $columns, string $delimiter): string
    {
        return implode($delimiter, array_map(function ($columns) {
            return self::quoteIdentifier($columns);
        }, $columns));
    }
}
