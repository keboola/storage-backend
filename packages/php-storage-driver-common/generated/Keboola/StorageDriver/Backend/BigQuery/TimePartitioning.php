<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/backend/bigQuery.proto

namespace Keboola\StorageDriver\Backend\BigQuery;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 **
 * Based on Bigquery REST API v2
 * https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TimePartitioning
 *
 * Generated from protobuf message <code>keboola.storageDriver.backend.bigQuery.TimePartitioning</code>
 */
class TimePartitioning extends \Google\Protobuf\Internal\Message
{
    /**
     * Generated from protobuf field <code>string type = 1;</code>
     */
    protected $type = '';
    /**
     * Generated from protobuf field <code>string expirationMs = 2;</code>
     */
    protected $expirationMs = '';
    /**
     * Generated from protobuf field <code>string field = 3;</code>
     */
    protected $field = '';

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $type
     *     @type string $expirationMs
     *     @type string $field
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Backend\BigQuery::initOnce();
        parent::__construct($data);
    }

    /**
     * Generated from protobuf field <code>string type = 1;</code>
     * @return string
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * Generated from protobuf field <code>string type = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setType($var)
    {
        GPBUtil::checkString($var, True);
        $this->type = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string expirationMs = 2;</code>
     * @return string
     */
    public function getExpirationMs()
    {
        return $this->expirationMs;
    }

    /**
     * Generated from protobuf field <code>string expirationMs = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setExpirationMs($var)
    {
        GPBUtil::checkString($var, True);
        $this->expirationMs = $var;

        return $this;
    }

    /**
     * Generated from protobuf field <code>string field = 3;</code>
     * @return string
     */
    public function getField()
    {
        return $this->field;
    }

    /**
     * Generated from protobuf field <code>string field = 3;</code>
     * @param string $var
     * @return $this
     */
    public function setField($var)
    {
        GPBUtil::checkString($var, True);
        $this->field = $var;

        return $this;
    }

}
