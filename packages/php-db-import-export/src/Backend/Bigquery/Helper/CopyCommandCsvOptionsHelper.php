<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Bigquery\Helper;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;
use Keboola\TableBackendUtils\Escaping\Bigquery\BigqueryQuote;

final class CopyCommandCsvOptionsHelper
{
    /**
     * @return string[]
     */
    public static function getCsvCopyCommandOptions(
        ImportOptionsInterface $importOptions,
        CsvOptions $csvOptions
    ): array {
        $options = [
            sprintf('field_delimiter=%s', BigqueryQuote::quote($csvOptions->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('skip_leading_rows=%s', $importOptions->getNumberOfIgnoredLines());
        }
        if ($csvOptions->getEnclosure()) {
            $options[] = sprintf('quote=%s', BigqueryQuote::quote($csvOptions->getEnclosure()));
            $options[] = 'allow_quoted_newlines=true';
        }

        return $options;
    }
}
