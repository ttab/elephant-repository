# Elephant logging

All error responses in Elephant get logged. Some log fields can be deduced from the error itself; like error message, error code, status code, and error metadata. Other information is derived from the request; like what service and method is being called, and what the sub of the client is. A third kind of contextual information is provided by the code handling the request, like the the document UUID the request acts on.

To be able to add context to logs elephantine adds `WithLogMetadata()` to set up a context with log metadata. Any part of the application handling the request can then `SetLogMetadata()` and the added metadata will be included with every log entry that uses that context, not only error response logging.

Keep in mind that adding log metadata isn't always the correct solution. If the information cannot be directly derived from the incoming request it's probably the wrong solution. Other alternatives are:

* Set the log field when making the log call.
* Return information with the error value.
* Create a child logger to pass to a function or subcomponent.

The first solution is the best when you want to log an error or diagnostic details that shouldn't be seen as request-global. An example would be if the requests involves processing a batch of items; if one of them fails you'd like to log information about the error together with the document UUID, but not necessarily fail the entire request.

Retiurning information with the error has the advantage that you'll both communicate the error details to the client and log them. Though any error metadata provided will be nested under "err_meta" instead of being used as top level keys.

Creating a child logger is probably not something that will be common, but it's useful when you want to affect all logging down the call-stack, or for a component in your application. 

* Context log metadata sets request global fields, affecting all future logging in the context.
* Returned error information will be logged and returned to the client, but doesn't affect other parts of the application.
* Child loggers will enforce that anyone that uses that logger always will log with specified fields set, but doesn't affect things like error response logging. 

## Example log entries

Errors that resolve to "400 Bad Request" will be logged at the INFO level, anything above status code 500 will be logged as ERROR, and the rest will be logged as WARN.

```
{
  "time": "2023-05-29T09:36:19.754535456+02:00",
  "level": "WARN",
  "msg": "error response",
  "err_code": "not_found",
  "err": "no such document",
  "status_code": 404,
  "service": "Documents",
  "method": "Get",
  "document_uuid": "98cedf81-ce8d-4dc4-bcbc-6091a756cc5c",
  "sub": "user://tt/hugo"
}
{
  "time": "2023-05-29T09:36:25.146281517+02:00",
  "level": "INFO",
  "msg": "error response",
  "err_code": "invalid_argument",
  "status_code": 400,
  "err": "uuid invalid UUID length: 37",
  "err_meta": {
    "argument": "uuid"
  },
  "sub": "user://tt/hugo",
  "service": "Documents",
  "method": "Get",
  "document_uuid": "98cedf81-ce8d-4dc4-bcbc-6091a756cc5cs"
}
{
  "time": "2023-05-29T09:47:34.406636284+02:00",
  "level": "WARN",
  "msg": "error response",
  "err_code": "permission_denied",
  "err": "no read permission",
  "status_code": 403,
  "scopes": "doc_write doc_delete schema_admin publish workflow_admin read_eventlog search",
  "sub": "user://tt/hugo",
  "service": "Documents",
  "method": "Get",
  "document_uuid": "98cedf81-ce8d-4dc4-bcbc-6091a756cc5c"
}
{
  "time": "2023-05-29T10:46:24.045577678+02:00",
  "level": "WARN",
  "msg": "error response",
  "err_code": "unauthenticated",
  "err": "no anonymous access allowed",
  "status_code": 401
}
```

## Point of friction

Say that you're processing a batch of documents and want to fail the entire request if one document fails. Then the only way to get the "document_uuid" logged with the error response would be to `SetLogMetadata()` and then return your error. If you also want to communicate the failing UUID to the caller it would have to be set as error meta as well.

This could be solved by logging information about the offending document first, and then also return an error. This will lead to duplicate log entries describing the error as it violates the "log or return an error, not both"-principle, but might be fine if done consciously.

It could also be solved by "promoting" well known fields from error metadata to top level log fields, but that's smells a bit like magic and might confuse the issue of what actually got sent to the client. It might also lead to use where information is returned to the client just to get it logged, not because it is of any use to the client.
