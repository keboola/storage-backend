<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/info.proto

namespace Keboola\StorageDriver\Command\Info;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.info.ObjectInfoResponse</code>
 */
class ObjectInfoResponse extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>repeated string path = 1;</code>
     */
    private $path;
    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ObjectType objectType = 2;</code>
     */
    protected $objectType = 0;
    protected $objectInfo;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type array<string>|\Google\Protobuf\Internal\RepeatedField $path
     *     @type int $objectType
     *     @type \Keboola\StorageDriver\Command\Info\DatabaseInfo $databaseInfo
     *     @type \Keboola\StorageDriver\Command\Info\SchemaInfo $schemaInfo
     *     @type \Keboola\StorageDriver\Command\Info\ViewInfo $viewInfo
     *     @type \Keboola\StorageDriver\Command\Info\TableInfo $tableInfo
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Info::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>repeated string path = 1;</code>
     * @return \Google\Protobuf\Internal\RepeatedField
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
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
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ObjectType objectType = 2;</code>
     * @return int
     */
    public function getObjectType()
    {
        return $this->objectType;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ObjectType objectType = 2;</code>
     * @param int $var
     * @return $this
     */
    public function setObjectType($var)
    {
        GPBUtil::checkEnum($var, \Keboola\StorageDriver\Command\Info\ObjectType::class);
        $this->objectType = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.DatabaseInfo databaseInfo = 3;</code>
     * @return \Keboola\StorageDriver\Command\Info\DatabaseInfo|null
     */
    public function getDatabaseInfo()
    {
        return $this->readOneof(3);
    }

    public function hasDatabaseInfo()
    {
        return $this->hasOneof(3);
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.DatabaseInfo databaseInfo = 3;</code>
     * @param \Keboola\StorageDriver\Command\Info\DatabaseInfo $var
     * @return $this
     */
    public function setDatabaseInfo($var)
    {
        GPBUtil::checkMessage($var, \Keboola\StorageDriver\Command\Info\DatabaseInfo::class);
        $this->writeOneof(3, $var);

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.SchemaInfo schemaInfo = 4;</code>
     * @return \Keboola\StorageDriver\Command\Info\SchemaInfo|null
     */
    public function getSchemaInfo()
    {
        return $this->readOneof(4);
    }

    public function hasSchemaInfo()
    {
        return $this->hasOneof(4);
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.SchemaInfo schemaInfo = 4;</code>
     * @param \Keboola\StorageDriver\Command\Info\SchemaInfo $var
     * @return $this
     */
    public function setSchemaInfo($var)
    {
        GPBUtil::checkMessage($var, \Keboola\StorageDriver\Command\Info\SchemaInfo::class);
        $this->writeOneof(4, $var);

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ViewInfo viewInfo = 5;</code>
     * @return \Keboola\StorageDriver\Command\Info\ViewInfo|null
     */
    public function getViewInfo()
    {
        return $this->readOneof(5);
    }

    public function hasViewInfo()
    {
        return $this->hasOneof(5);
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.ViewInfo viewInfo = 5;</code>
     * @param \Keboola\StorageDriver\Command\Info\ViewInfo $var
     * @return $this
     */
    public function setViewInfo($var)
    {
        GPBUtil::checkMessage($var, \Keboola\StorageDriver\Command\Info\ViewInfo::class);
        $this->writeOneof(5, $var);

        return $this;
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.TableInfo tableInfo = 6;</code>
     * @return \Keboola\StorageDriver\Command\Info\TableInfo|null
     */
    public function getTableInfo()
    {
        return $this->readOneof(6);
    }

    public function hasTableInfo()
    {
        return $this->hasOneof(6);
    }

    /**
     * Generated from protobuf field <code>.keboola.storageDriver.command.info.TableInfo tableInfo = 6;</code>
     * @param \Keboola\StorageDriver\Command\Info\TableInfo $var
     * @return $this
     */
    public function setTableInfo($var)
    {
        GPBUtil::checkMessage($var, \Keboola\StorageDriver\Command\Info\TableInfo::class);
        $this->writeOneof(6, $var);

        return $this;
    }

    /**
     * @return string
     */
    public function getObjectInfo()
    {
        return $this->whichOneof("objectInfo");
    }

}
