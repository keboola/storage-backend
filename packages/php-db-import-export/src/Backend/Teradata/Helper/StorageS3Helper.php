<?php

declare(strict_types=1);

namespace Keboola\Db\ImportExport\Backend\Teradata\Helper;

use Keboola\Db\ImportExport\Backend\Helper\BackendHelper as BaseHelper;
use Keboola\Db\ImportExport\Storage\S3\SourceFile;

final class StorageS3Helper extends BaseHelper
{
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

    public static function isMultipartFile(SourceFile $source): bool
    {
        $entries = $source->getManifestEntries();
        if (count($entries) === 0) {
            // no entries -> no data to load
            return false;
        }

        // docs say 6, but my files are created with 5
        return (bool) preg_match('/(?<filePath>.*)\/F(?<fileNumber>[0-9]{5,6})/', $entries[0], $out);
    }

    /**
     * extracts filename and prefix from s3 url - removing bucket, protocol and Fxxx suffix
     * @return string[]
     */
    public static function buildPrefixAndObject(SourceFile $source): array
    {
        // docs say 6, but my files are created with 5
        $entries = $source->getManifestEntries();
        preg_match('/(?<filePath>.*)\/F(?<fileNumber>[0-9]{5,6})/', $entries[0], $out);

        $filePath = $out['filePath'] ?? '';
        $filePath = str_replace(($source->getS3Prefix() . '/'), '', $filePath);

        $exploded = explode('/', $filePath);
        $object = end($exploded);
        // get all the parts of exploded path but without the last thing - the filename
        $prefix = implode('/', array_slice($exploded, 0, -1));
        // prefix should end with / but only if it exists
        $prefix = $prefix ? ($prefix . '/') : '';
        return [$prefix, $object];
    }
}
