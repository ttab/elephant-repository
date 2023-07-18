# Permissions

Permissions are controlled by a set of scopes and document ACL lists.

* ACL read access checks can be circumvented by: doc_read_all, doc_admin
* ACL write access checks can be circumvented by: doc_admin

## Scopes

``` go
	ScopeDocumentAdmin   = "doc_admin"
	ScopeDocumentReadAll = "doc_read_all"
	ScopeDocumentRead    = "doc_read"
	ScopeDocumentDelete  = "doc_delete"
	ScopeDocumentWrite   = "doc_write"
	ScopeDocumentImport  = "doc_import"
	ScopeEventlogRead    = "eventlog_read"
	ScopeMetricsAdmin    = "metrics_admin"
	ScopeMetricsWrite    = "metrics_write"
	ScopeReportAdmin     = "report_admin"
	ScopeReportRun       = "report_run"
	ScopeSchemaAdmin     = "schema_admin"
	ScopeSchemaRead      = "schema_read"
	ScopeWorkflowAdmin   = "workflow_admin"
```

Some scopes can have subscopes, like in the case of "metrics_write" where access can be given to write a specific metric kind through the scope "metrics_write:word_count".

## Documents

### GetStatusHistory

Requires one of: doc_read, doc_read_all, doc_admin

ACL read access check.

### GetPermissions

Requires one of: doc_read, doc_write, doc_delete, doc_read_all, doc_admin

All clients with document scopes can read permissions.

### Eventlog

Requires one of: eventlog_read, doc_admin

### Delete

Requires one of: doc_delete, doc_admin

ACL write access check.

### Get

Requires one of: doc_read, doc_read_all, doc_admin

ACL read access check.

### GetHistory

Requires one of: doc_read, doc_read_all, doc_admin

ACL read access check.

### GetMeta

Requires one of: doc_read, doc_read_all, doc_admin

ACL read access check.

### Update

Requires one of: doc_write, doc_admin

ACL write access check.

Use of import directives requires one of: doc_import, doc_admin

### Validate

No specific permissions required.

### Lock

Requires one of: doc_write, doc_delete, doc_admin

ACL write access check.

### ExtendLock

Requires one of: doc_write, doc_delete, doc_admin

ACL write access check.

### Unlock

Requires one of: doc_write, doc_delete, doc_admin

ACL write access check.
