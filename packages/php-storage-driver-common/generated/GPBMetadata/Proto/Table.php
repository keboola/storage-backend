<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace GPBMetadata\Proto;

class Table
{
    public static $is_initialized = false;

    public static function initOnce() {
        $pool = \Google\Protobuf\Internal\DescriptorPool::getGeneratedPool();

        if (static::$is_initialized == true) {
          return;
        }
        \GPBMetadata\Google\Protobuf\Any::initOnce();
        \GPBMetadata\Google\Protobuf\Struct::initOnce();
        \GPBMetadata\Proto\Info::initOnce();
        \GPBMetadata\Proto\Backend\BigQuery::initOnce();
        $pool->internalAddGeneratedFile(
            '
�C
proto/table.proto#keboola.storageDriver.command.tablegoogle/protobuf/struct.protoproto/info.protoproto/backend/bigQuery.proto"�
TableColumnShared
name (	
type (	
length (	
nullable (
default (	"
meta (2.google.protobuf.Any*
TeradataTableColumnMeta
isLatin ("�
CreateTableCommand
path (	
	tableName (	G
columns (26.keboola.storageDriver.command.table.TableColumnShared
primaryKeysNames (	"
meta (2.google.protobuf.Any
SynapseTableMeta�
BigQueryTableMetaR
timePartitioning (28.keboola.storageDriver.backend.bigQuery.TimePartitioningT
rangePartitioning (29.keboola.storageDriver.backend.bigQuery.RangePartitioning
requirePartitionFilter (F

clustering (22.keboola.storageDriver.backend.bigQuery.Clustering"M
DropTableCommand
path (	
	tableName (	
ignoreErrors (B"�
AddColumnCommand
path (	
	tableName (	P
columnDefinition (26.keboola.storageDriver.command.table.TableColumnShared"�
AlterColumnCommand
path (	
	tableName (	P
desiredDefiniton (26.keboola.storageDriver.command.table.TableColumnShared
attributesToUpdate (	"H
DropColumnCommand
path (	
	tableName (	

columnName (	"Q
AddPrimaryKeyCommand
path (	
	tableName (	
primaryKeysNames (	"8
DropPrimaryKeyCommand
path (	
	tableName (	"�
PreviewTableCommand
path (	
	tableName (	
columns (	V
orderBy (2E.keboola.storageDriver.command.table.ImportExportShared.ExportOrderByV
filters (2E.keboola.storageDriver.command.table.ImportExportShared.ExportFilters"�
PreviewTableResponse
columns (	K
rows (2=.keboola.storageDriver.command.table.PreviewTableResponse.Row�
RowU
columns (2D.keboola.storageDriver.command.table.PreviewTableResponse.Row.ColumnX
Column

columnName (	%
value (2.google.protobuf.Value
isTruncated ("�
ImportExportShared�
TableWhereFilter
columnsName (	c
operator (2Q.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilter.Operator
values (	R
dataType (2@.keboola.storageDriver.command.table.ImportExportShared.DataType":
Operator
eq 
ne
gt
ge
lt
le(
Table
path (	
	tableName (	�
ImportOptions
timestampColumn (	)
!convertEmptyValuesToNullOnColumns (	d

importType (2P.keboola.storageDriver.command.table.ImportExportShared.ImportOptions.ImportType
numberOfIgnoredLines (b
	dedupType (2O.keboola.storageDriver.command.table.ImportExportShared.ImportOptions.DedupType
dedupColumnsNames (	l
importStrategy (2T.keboola.storageDriver.command.table.ImportExportShared.ImportOptions.ImportStrategyd

createMode (2P.keboola.storageDriver.command.table.ImportExportShared.ImportOptions.CreateMode
importAsNull	 (	"<

ImportType
FULL 
INCREMENTAL
VIEW	
CLONE"Q
	DedupType
UPDATE_DUPLICATES 
INSERT_DUPLICATES
FAIL_ON_DUPLICATES":
ImportStrategy
STRING_TABLE 
USER_DEFINED_TABLE"%

CreateMode

CREATE 
REPLACE�
ExportOptions
isCompressed (
columnsToExport (	V
orderBy (2E.keboola.storageDriver.command.table.ImportExportShared.ExportOrderByV
filters (2E.keboola.storageDriver.command.table.ImportExportShared.ExportFilters�
ExportFilters
limit (
changeSince (	
changeUntil (	
fulltextSearch (	^
whereFilters (2H.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilter�
ExportOrderBy

columnName (	Z
order (2K.keboola.storageDriver.command.table.ImportExportShared.ExportOrderBy.OrderR
dataType (2@.keboola.storageDriver.command.table.ImportExportShared.DataType"
Order
ASC 
DESCK
S3Credentials
key (	
secret (	
region (	
token (	K
ABSCredentials
accountName (	
sasToken (	

accountKey (	-
GCSCredentials
key (	
secret (	8
FilePath
root (	
path (	
fileName (	"R
DataType

STRING 
INTEGER

DOUBLE

BIGINT
REAL
DECIMAL"(
FileProvider
S3 
ABS
GCS"

FileFormat
CSV "�	
TableImportFromFileCommandZ
fileProvider (2D.keboola.storageDriver.command.table.ImportExportShared.FileProviderV

fileFormat (2B.keboola.storageDriver.command.table.ImportExportShared.FileFormat/
formatTypeOptions (2.google.protobuf.AnyR
filePath (2@.keboola.storageDriver.command.table.ImportExportShared.FilePath-
fileCredentials (2.google.protobuf.AnyR
destination (2=.keboola.storageDriver.command.table.ImportExportShared.Table\\
importOptions (2E.keboola.storageDriver.command.table.ImportExportShared.ImportOptions"
meta (2.google.protobuf.Any�
CsvTypeOptions
columnsNames (	
	delimiter (	
	enclosure (	
	escapedBy (	m

sourceType (2Y.keboola.storageDriver.command.table.TableImportFromFileCommand.CsvTypeOptions.SourceTypeo
compression (2Z.keboola.storageDriver.command.table.TableImportFromFileCommand.CsvTypeOptions.Compression"=

SourceType
SINGLE_FILE 
SLICED_FILE
	DIRECTORY"!
Compression
NONE 
GZIP�
TeradataTableImportMeta|
importAdapter (2e.keboola.storageDriver.command.table.TableImportFromFileCommand.TeradataTableImportMeta.ImportAdapter"
ImportAdapter
TPT "�
TableImportResponse
importedRowsCount (
tableRowsCount (
tableSizeBytes (N
timers (2>.keboola.storageDriver.command.table.TableImportResponse.Timer
importedColumns (	"
meta (2.google.protobuf.Any\'
Timer
name (	
duration (	d
TeradataTableImportMeta
	importLog (	
errorTable1records (	
errorTable2records (	"�
TableImportFromTableCommandc
source (2S.keboola.storageDriver.command.table.TableImportFromTableCommand.SourceTableMappingR
destination (2=.keboola.storageDriver.command.table.ImportExportShared.Table\\
importOptions (2E.keboola.storageDriver.command.table.ImportExportShared.ImportOptions�
SourceTableMapping
path (	
	tableName (	
seconds (^
whereFilters (2H.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilter
limit (y
columnMappings (2a.keboola.storageDriver.command.table.TableImportFromTableCommand.SourceTableMapping.ColumnMappingH
ColumnMapping
sourceColumnName (	
destinationColumnName (	"�
TableExportToFileCommandM
source (2=.keboola.storageDriver.command.table.ImportExportShared.TableZ
fileProvider (2D.keboola.storageDriver.command.table.ImportExportShared.FileProviderV

fileFormat (2B.keboola.storageDriver.command.table.ImportExportShared.FileFormatR
filePath (2@.keboola.storageDriver.command.table.ImportExportShared.FilePath-
fileCredentials (2.google.protobuf.Any\\
exportOptions (2E.keboola.storageDriver.command.table.ImportExportShared.ExportOptions"
meta (2.google.protobuf.Any�
TeradataTableExportMetaz
exportAdapter (2c.keboola.storageDriver.command.table.TableExportToFileCommand.TeradataTableExportMeta.ExportAdapter"
ExportAdapter
TPT "]
TableExportToFileResponse@
	tableInfo (2-.keboola.storageDriver.command.info.TableInfo"�
DeleteTableRowsCommand
path (	
	tableName (	
changeSince (	
changeUntil (	^
whereFilters (2H.keboola.storageDriver.command.table.ImportExportShared.TableWhereFilterm
whereRefTableFilters (2O.keboola.storageDriver.command.table.DeleteTableRowsCommand.WhereRefTableFilter�
WhereRefTableFilter
column (	j
operator (2X.keboola.storageDriver.command.table.DeleteTableRowsCommand.WhereRefTableFilter.Operator
refPath (	
refTable (	
	refColumn (	"
Operator
IN 

NOT_IN"c
DeleteTableRowsResponse
deletedRowsCount (
tableRowsCount (
tableSizeBytes ("�
 CreateTableFromTimeTravelCommandh
source (2X.keboola.storageDriver.command.table.CreateTableFromTimeTravelCommand.SourceTableMappingR
destination (2=.keboola.storageDriver.command.table.ImportExportShared.Table
	timestamp (5
SourceTableMapping
path (	
	tableName (	"<
CreateProfileTableCommand
path (	
	tableName (	"�
CreateProfileTableResponse
path (	
	tableName (	
profile (	W
columns (2F.keboola.storageDriver.command.table.CreateProfileTableResponse.Column\'
Column
name (	
profile (	bproto3'
        , true);

        static::$is_initialized = true;
    }
}

