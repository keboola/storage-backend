<?php

declare(strict_types=1);

namespace Keboola\TableBackendUtils\Auth\Grant\Synapse;

/**
 * Source
 * https://docs.microsoft.com/en-us/sql/t-sql/statements/permissions-grant-deny-revoke-azure-sql-data-warehouse-parallel-data-warehouse?view=aps-pdw-2016-au7
 */
final class Permission
{
    // Database Level Permissions
    // Database level permissions can be granted,denied, and revoked
    // from database users and user-defined database roles.
    //

    // Permissions that apply to all database classes
    public const GRANT_ALTER_ANY_EXTERNAL_DATA_SOURCE = 'ALTER ANY EXTERNAL DATA SOURCE';
    public const GRANT_ALTER_ANY_EXTERNAL_FILE_FORMAT = 'ALTER ANY EXTERNAL FILE FORMAT';
    public const GRANT_ADMINISTER_DATABASE_BULK_OPERATIONS = 'ADMINISTER DATABASE BULK OPERATIONS';
    public const GRANT_CONTROL = 'CONTROL';
    public const GRANT_ALTER = 'ALTER';
    public const GRANT_VIEW_DEFINITION = 'VIEW DEFINITION';
    // Permissions that apply to all database classes except users
    public const GRANT_TAKE_OWNERSHIP = 'TAKE OWNERSHIP';
    // Permissions that apply only to databases
    public const GRANT_ALTER_ANY_DATASPACE = 'ALTER ANY DATASPACE';
    public const GRANT_ALTER_ANY_ROLE = 'ALTER ANY ROLE';
    public const GRANT_ALTER_ANY_SCHEMA = 'ALTER ANY SCHEMA';
    public const GRANT_ALTER_ANY_USER = 'ALTER ANY USER';
    public const GRANT_BACKUP_DATABASE = 'BACKUP DATABASE';
    public const GRANT_CREATE_PROCEDURE = 'CREATE PROCEDURE';
    public const GRANT_CREATE_ROLE = 'CREATE ROLE';
    public const GRANT_CREATE_SCHEMA = 'CREATE SCHEMA';
    public const GRANT_CREATE_TABLE = 'CREATE TABLE';
    public const GRANT_CREATE_VIEW = 'CREATE VIEW';
    public const GRANT_SHOWPLAN = 'SHOWPLAN';
    // Permissions that apply to databases, schemas, and objects
//    public const GRANT_ALTER = 'ALTER';
    public const GRANT_DELETE = 'DELETE';
    public const GRANT_EXECUTE = 'EXECUTE';
    public const GRANT_INSERT = 'INSERT';
    public const GRANT_SELECT = 'SELECT';
    public const GRANT_UPDATE = 'UPDATE';
    public const GRANT_REFERENCES = 'REFERENCES';
}
