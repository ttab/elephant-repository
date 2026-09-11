# Workflow and ACL changes are folded onto the event that caused them

Workflow state changes used to be emitted as standalone `workflow` events, and
an ACL update that came with a new document version used to be emitted as a
standalone `acl` event. Since v1.8.0 both are carried as fields
(`workflow_state`/`workflow_checkpoint`, and `acl`) on the `document` or
`status` event that caused them, and the standalone events are gone.
`--emit-workflow-event` and `--emit-acl-event` re-emit the legacy events
alongside the folded fields, as a transition aid for external consumers, and
are slated for removal.

## Why

The split shape had a correctness defect, not just a verbosity problem: the
version event was emitted *before* the accompanying ACL event, so a consumer
could observe a new version before the permissions it was created with, and act
on it. Two events describing one transaction cannot be made atomic for a
consumer that reads them in order — the only fix is for there to be one event.

The same argument applies to workflow state, which is derived from the status
or version update that triggered it and has no independent existence.

## Consequences

- **Re-splitting these into standalone events reintroduces the ordering hole.**
  If a consumer asks for a separate `acl` event after every version, the answer
  is the compatibility flag, not a change to the event shape.
- Standalone `acl` events still exist for an ACL update made on its own, with no
  new version, and on archive restore. Those are genuinely separate
  transactions.
- Consumers branching on `event == "workflow"` stop matching. The
  `workflow_state` table is still written as before, so nothing that reads the
  database is affected.
