<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table\ImportExportShared;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.table.ImportExportShared.ExportOrderBy</code>
 */
class ExportOrderBy extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string columnName = 1;</code>
     */
    protected $columnName = '';
    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.ImportExportShared.ExportOrderBy.Order order = 2;</code>
     */
    protected $order = 0;
    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.ImportExportShared.DataType dataType = 3;</code>
     */
    protected $dataType = 0;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $columnName
     *     @type int $order
     *     @type int $dataType
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Table::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string columnName = 1;</code>
     * @return string
     */
    public function getColumnName()
    {
        return $this->columnName;
    }

    /**
     * Generated from protobuf field <code>string columnName = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setColumnName($var)
    {
        GPBUtil::checkString($var, True);
        $this->columnName = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.ImportExportShared.ExportOrderBy.Order order = 2;</code>
     * @return int
     */
    public function getOrder()
    {
        return $this->order;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.ImportExportShared.ExportOrderBy.Order order = 2;</code>
     * @param int $var
     * @return $this
     */
    public function setOrder($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Table\ImportExportShared\ExportOrderBy\Order::class);
        $this->order = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.ImportExportShared.DataType dataType = 3;</code>
     * @return int
     */
    public function getDataType()
    {
        return $this->dataType;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.table.ImportExportShared.DataType dataType = 3;</code>
     * @param int $var
     * @return $this
     */
    public function setDataType($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Table\ImportExportShared\DataType::class);
        $this->dataType = $var;

        return $this;
    }

}

// Adding a class alias for backwards compatibility with the previous class name.
class_alias(ExportOrderBy::class, \Keboola\StorageDriver\Command\Table\ImportExportShared_ExportOrderBy::class);
