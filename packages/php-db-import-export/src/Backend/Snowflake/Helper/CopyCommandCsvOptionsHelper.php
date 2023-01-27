<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Helper;

use Keboola\CsvOptions\CsvOptions;
use Keboola\Db\ImportExport\ImportOptionsInterface;

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
            sprintf('FIELD_DELIMITER = %s', QuoteHelper::quote($csvOptions->getDelimiter())),
        ];

        if ($importOptions->getNumberOfIgnoredLines() > 0) {
            $options[] = sprintf('SKIP_HEADER = %d', $importOptions->getNumberOfIgnoredLines());
        }

        if ($csvOptions->getEnclosure()) {
            $options[] = sprintf('FIELD_OPTIONALLY_ENCLOSED_BY = %s', QuoteHelper::quote($csvOptions->getEnclosure()));
            $options[] = 'ESCAPE_UNENCLOSED_FIELD = NONE';
        } elseif ($csvOptions->getEscapedBy()) {
            $options[] = sprintf('ESCAPE_UNENCLOSED_FIELD = %s', QuoteHelper::quote($csvOptions->getEscapedBy()));
        }
        return $options;
    }
}
