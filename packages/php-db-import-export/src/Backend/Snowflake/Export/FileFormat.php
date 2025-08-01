<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Snowflake\Export;

use Keboola\Db\ImportExport\ExportFileType;
use Keboola\Db\ImportExport\ExportOptionsInterface;

class FileFormat
{
    public static function getFileFormatForCopyInto(
        ExportOptionsInterface $options,
    ): string {
        $timestampFormat = 'YYYY-MM-DD HH24:MI:SS';
        if (in_array(Exporter::FEATURE_FRACTIONAL_SECONDS, $options->features(), true)) {
            $timestampFormat = 'YYYY-MM-DD HH24:MI:SS.FF9';
        }

        return match ($options->getFileType()) {
            ExportFileType::CSV => sprintf(
                <<<EOD
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
    %s
    TIMESTAMP_FORMAT = '%s',
    NULL_IF = ()
EOD,
                $options->isCompressed() ? 'COMPRESSION=GZIP' : 'COMPRESSION=NONE',
                $timestampFormat,

            ),
            ExportFileType::PARQUET => sprintf(
                <<<EOD
    TYPE=PARQUET
    %s
EOD,
                $options->isCompressed() ? 'COMPRESSION=SNAPPY' : 'COMPRESSION=NONE',
            ),
        };
    }
}
