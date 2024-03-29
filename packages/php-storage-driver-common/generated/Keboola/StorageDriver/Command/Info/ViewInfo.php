<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/info.proto

namespace Keboola\StorageDriver\Command\Info;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.info.ViewInfo</code>
 */
class ViewInfo extends \Google\Protobuf\Internal\Message
{
    /**
     * path where view is located
     *
     * Generated from protobuf field <code>repeated string path = 1;</code>
     */
    private $path;
    /**
     * view name
     *
     * Generated from protobuf field <code>string viewName = 2;</code>
     */
    protected $viewName = '';
    /**
     * table columns definitions
     *
     * Generated from protobuf field <code>repeated .keboola.storageDriver.command.info.TableInfo.TableColumn columns = 3;</code>
     */
    private $columns;
    /**
     * primary key columns names
     *
     * Generated from protobuf field <code>repeated string primaryKeysNames = 4;</code>
     */
    private $primaryKeysNames;
    /**
     * Generated from protobuf field <code>int64 rowsCount = 5;</code>
     */
    protected $rowsCount = 0;
    /**
     * metadata specific for each backend
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 6;</code>
     */
    protected $meta = null;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type array<string>|\Google\Protobuf\Internal\RepeatedField $path
     *           path where view is located
     *     @type string $viewName
     *           view name
     *     @type array<\Keboola\StorageDriver\Command\Info\TableInfo\TableColumn>|\Google\Protobuf\Internal\RepeatedField $columns
     *           table columns definitions
     *     @type array<string>|\Google\Protobuf\Internal\RepeatedField $primaryKeysNames
     *           primary key columns names
     *     @type int|string $rowsCount
     *     @type \Google\Protobuf\Any $meta
     *           metadata specific for each backend
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Info::initOnce();
        parent::__construct($data);
    }

    /**
     * path where view is located
     *
     * Generated from protobuf field <code>repeated string path = 1;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
     * path where view is located
     *
     * Generated from protobuf field <code>repeated string path = 1;</code>
     * @param array<string>|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setPath($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::STRING);
        $this->path = $arr;

        return $this;
    }

    /**
     * view name
     *
     * Generated from protobuf field <code>string viewName = 2;</code>
     * @return string
     */
    public function getViewName()
    {
        return $this->viewName;
    }

    /**
     * view name
     *
     * Generated from protobuf field <code>string viewName = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setViewName($var)
    {
        GPBUtil::checkString($var, True);
        $this->viewName = $var;

        return $this;
    }

    /**
     * table columns definitions
     *
     * Generated from protobuf field <code>repeated .keboola.storageDriver.command.info.TableInfo.TableColumn columns = 3;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getColumns()
    {
        return $this->columns;
    }

    /**
     * table columns definitions
     *
     * Generated from protobuf field <code>repeated .keboola.storageDriver.command.info.TableInfo.TableColumn columns = 3;</code>
     * @param array<\Keboola\StorageDriver\Command\Info\TableInfo\TableColumn>|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setColumns($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::MESSAGE, \Keboola\StorageDriver\Command\Info\TableInfo\TableColumn::class);
        $this->columns = $arr;

        return $this;
    }

    /**
     * primary key columns names
     *
     * Generated from protobuf field <code>repeated string primaryKeysNames = 4;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getPrimaryKeysNames()
    {
        return $this->primaryKeysNames;
    }

    /**
     * primary key columns names
     *
     * Generated from protobuf field <code>repeated string primaryKeysNames = 4;</code>
     * @param array<string>|\Google\Protobuf\Internal\RepeatedField $var
     * @return $this
     */
    public function setPrimaryKeysNames($var)
    {
        $arr = GPBUtil::checkRepeatedField($var, \Google\Protobuf\Internal\GPBType::STRING);
        $this->primaryKeysNames = $arr;

        return $this;
    }

    /**
     * Generated from protobuf field <code>int64 rowsCount = 5;</code>
     * @return int|string
     */
    public function getRowsCount()
    {
        return $this->rowsCount;
    }

    /**
     * Generated from protobuf field <code>int64 rowsCount = 5;</code>
     * @param int|string $var
     * @return $this
     */
    public function setRowsCount($var)
    {
        GPBUtil::checkInt64($var);
        $this->rowsCount = $var;

        return $this;
    }

    /**
     * metadata specific for each backend
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 6;</code>
     * @return \Google\Protobuf\Any|null
     */
    public function getMeta()
    {
        return $this->meta;
    }

    public function hasMeta()
    {
        return isset($this->meta);
    }

    public function clearMeta()
    {
        unset($this->meta);
    }

    /**
     * metadata specific for each backend
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 6;</code>
     * @param \Google\Protobuf\Any $var
     * @return $this
     */
    public function setMeta($var)
    {
        GPBUtil::checkMessage($var, \Google\Protobuf\Any::class);
        $this->meta = $var;

        return $this;
    }

}

