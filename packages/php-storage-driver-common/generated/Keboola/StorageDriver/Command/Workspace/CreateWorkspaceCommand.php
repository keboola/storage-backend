<?php
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/workspace.proto

namespace Keboola\StorageDriver\Command\Workspace;

use Google\Protobuf\Internal\GPBType;
use Google\Protobuf\Internal\RepeatedField;
use Google\Protobuf\Internal\GPBUtil;

/**
 **
 * Command is used when new workspace is created in Keboola connection
 * Command will:
 * - create new workspace which is usually new schema
 * - create role which should be owner of schema and has all needed grant's
 * - create user with password which is used to connect into workspace
 * - make project role member of workspace role, so project user has also all needed grants on workspace as it will perform data loads
 * - if read only storage is supported project read only role is also assigned to workspace role so user can read from all project tables
 *
 * Generated from protobuf message <code>keboola.storageDriver.command.workspace.CreateWorkspaceCommand</code>
 */
class CreateWorkspaceCommand extends \Google\Protobuf\Internal\Message
{
    /**
     * static prefix of stack used
     *
     * Generated from protobuf field <code>string stackPrefix = 1;</code>
     */
    protected $stackPrefix = '';
    /**
     * Keboola Connection project id, id is currently numeric, but string here as this could change in the future
     *
     * Generated from protobuf field <code>string projectId = 2;</code>
     */
    protected $projectId = '';
    /**
     * Keboola Connection workspace id
     *
     * Generated from protobuf field <code>string workspaceId = 3;</code>
     */
    protected $workspaceId = '';
    /**
     * backend user associated with project
     *
     * Generated from protobuf field <code>string projectUserName = 4;</code>
     */
    protected $projectUserName = '';
    /**
     * backend role associated with project, role should contain all grants and be assigned to project user
     *
     * Generated from protobuf field <code>string projectRoleName = 5;</code>
     */
    protected $projectRoleName = '';
    /**
     * backend read only role associated with project, role has read access for all buckets in project and containing tables
     *
     * Generated from protobuf field <code>string projectReadOnlyRoleName = 6;</code>
     */
    protected $projectReadOnlyRoleName = '';
    /**
     * metadata specific for each backend
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 7;</code>
     */
    protected $meta = null;

    /**
     * Constructor.
     *
     * @param array $data {
     *     Optional. Data for populating the Message object.
     *
     *     @type string $stackPrefix
     *           static prefix of stack used
     *     @type string $projectId
     *           Keboola Connection project id, id is currently numeric, but string here as this could change in the future
     *     @type string $workspaceId
     *           Keboola Connection workspace id
     *     @type string $projectUserName
     *           backend user associated with project
     *     @type string $projectRoleName
     *           backend role associated with project, role should contain all grants and be assigned to project user
     *     @type string $projectReadOnlyRoleName
     *           backend read only role associated with project, role has read access for all buckets in project and containing tables
     *     @type \Google\Protobuf\Any $meta
     *           metadata specific for each backend
     * }
     */
    public function __construct($data = NULL) {
        \GPBMetadata\Proto\Workspace::initOnce();
        parent::__construct($data);
    }

    /**
     * static prefix of stack used
     *
     * Generated from protobuf field <code>string stackPrefix = 1;</code>
     * @return string
     */
    public function getStackPrefix()
    {
        return $this->stackPrefix;
    }

    /**
     * static prefix of stack used
     *
     * Generated from protobuf field <code>string stackPrefix = 1;</code>
     * @param string $var
     * @return $this
     */
    public function setStackPrefix($var)
    {
        GPBUtil::checkString($var, True);
        $this->stackPrefix = $var;

        return $this;
    }

    /**
     * Keboola Connection project id, id is currently numeric, but string here as this could change in the future
     *
     * Generated from protobuf field <code>string projectId = 2;</code>
     * @return string
     */
    public function getProjectId()
    {
        return $this->projectId;
    }

    /**
     * Keboola Connection project id, id is currently numeric, but string here as this could change in the future
     *
     * Generated from protobuf field <code>string projectId = 2;</code>
     * @param string $var
     * @return $this
     */
    public function setProjectId($var)
    {
        GPBUtil::checkString($var, True);
        $this->projectId = $var;

        return $this;
    }

    /**
     * Keboola Connection workspace id
     *
     * Generated from protobuf field <code>string workspaceId = 3;</code>
     * @return string
     */
    public function getWorkspaceId()
    {
        return $this->workspaceId;
    }

    /**
     * Keboola Connection workspace id
     *
     * Generated from protobuf field <code>string workspaceId = 3;</code>
     * @param string $var
     * @return $this
     */
    public function setWorkspaceId($var)
    {
        GPBUtil::checkString($var, True);
        $this->workspaceId = $var;

        return $this;
    }

    /**
     * backend user associated with project
     *
     * Generated from protobuf field <code>string projectUserName = 4;</code>
     * @return string
     */
    public function getProjectUserName()
    {
        return $this->projectUserName;
    }

    /**
     * backend user associated with project
     *
     * Generated from protobuf field <code>string projectUserName = 4;</code>
     * @param string $var
     * @return $this
     */
    public function setProjectUserName($var)
    {
        GPBUtil::checkString($var, True);
        $this->projectUserName = $var;

        return $this;
    }

    /**
     * backend role associated with project, role should contain all grants and be assigned to project user
     *
     * Generated from protobuf field <code>string projectRoleName = 5;</code>
     * @return string
     */
    public function getProjectRoleName()
    {
        return $this->projectRoleName;
    }

    /**
     * backend role associated with project, role should contain all grants and be assigned to project user
     *
     * Generated from protobuf field <code>string projectRoleName = 5;</code>
     * @param string $var
     * @return $this
     */
    public function setProjectRoleName($var)
    {
        GPBUtil::checkString($var, True);
        $this->projectRoleName = $var;

        return $this;
    }

    /**
     * backend read only role associated with project, role has read access for all buckets in project and containing tables
     *
     * Generated from protobuf field <code>string projectReadOnlyRoleName = 6;</code>
     * @return string
     */
    public function getProjectReadOnlyRoleName()
    {
        return $this->projectReadOnlyRoleName;
    }

    /**
     * backend read only role associated with project, role has read access for all buckets in project and containing tables
     *
     * Generated from protobuf field <code>string projectReadOnlyRoleName = 6;</code>
     * @param string $var
     * @return $this
     */
    public function setProjectReadOnlyRoleName($var)
    {
        GPBUtil::checkString($var, True);
        $this->projectReadOnlyRoleName = $var;

        return $this;
    }

    /**
     * metadata specific for each backend
     *
     * Generated from protobuf field <code>.google.protobuf.Any meta = 7;</code>
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
     * Generated from protobuf field <code>.google.protobuf.Any meta = 7;</code>
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
