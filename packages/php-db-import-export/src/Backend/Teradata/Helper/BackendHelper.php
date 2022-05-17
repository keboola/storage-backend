<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\Helper;

use Keboola\Db\ImportExport\Storage\S3\SourceFile;

final class BackendHelper
{
    public static function quoteValue(string $value): string
    {
        return "'" . addslashes($value) . "'";
    }

    public static function generateTempTableName(): string
    {
        return '__temp_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    public static function generateTempDedupTableName(): string
    {
        return '__temp_DEDUP_' . str_replace('.', '_', uniqid('csvimport', true));
    }

    /**
     * creates a wildcard string which should match all files in manifest
     * [file01.csv, file01.csv] => file0*
     * TODO
     *  - has to fix edgecases a) [1_file.csv, 2_file.csv] b) not all the files matched in WC have to be on s3
     * @throws \Keboola\Db\Import\Exception
     */
    public static function getMask(SourceFile $source): string
    {
        $entries = $source->getManifestEntries();
        if (count($entries) === 0) {
            // no entries -> no data to load
            return '';
        }
        // SourceDirectory returns fileName as directory/file.csv but SourceFile returns s3://bucket/directory/file.csv
        $toRemove = $source->getS3Prefix() . '/';
        $entriesAsArrays = [];
        $min = 99999;
        $minIndex = 0;
        foreach ($entries as $i => $entry) {
            $entry = str_replace($toRemove, '', $entry);
            $asArray = str_split($entry);
            $entriesAsArrays[] = $asArray;
            $thisSize = count($asArray);
            if ($thisSize < $min) {
                $min = $thisSize;
                $minIndex = $i;
            }
        }
        $out = [];

        foreach ($entriesAsArrays[$minIndex] as $index => $letter) {
            $match = true;

            foreach ($entriesAsArrays as $fileName) {
                if ($fileName[$index] !== $letter) {
                    $match = false;
                    break;
                }
            }
            $out[$index] = $match ? $letter : '*';
        }
        return implode('', $out);
    }
}
