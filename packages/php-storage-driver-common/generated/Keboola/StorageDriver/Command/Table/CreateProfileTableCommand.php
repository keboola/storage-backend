<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/table.proto

namespace Keboola\StorageDriver\Command\Table;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 * Generated from protobuf message <code>keboola.storageDriver.command.table.CreateProfileTableCommand</code>
 */
class CreateProfileTableCommand extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>repeated string path = 1;</code>
     */
    private $path;
    /**
     * Generated from protobuf field <code>string tableName = 2;</code>
     */
    protected $tableName = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type array<string>|\Google\Protobuf\Internal\RepeatedField $path
     *     @type string $tableName
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Table::initOnce();
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
     * Generated from protobuf field <code>string tableName = 2;</code>
     * @return string
     */
    public function getTableName()
    {
        return $this->tableName;
    }

    /**
     * Generated from protobuf field <code>string tableName = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setTableName($var)
    {
        GPBUtil::checkString($var, True);
        $this->tableName = $var;

        return $this;
    }

}

