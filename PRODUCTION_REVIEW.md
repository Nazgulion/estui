Code review follow-up — 2026-10-15 (v3.1.1)

Reviewed migration admission, transfer, verification, replacement/remap and recovery;
Elasticsearch transport and response handling; connection and local-access handlers;
checkpoint storage and process locking; configuration, logging and browser flows.
Each fix below was preceded by a failing regression reproducing the bug.

| Confirmed bug | Proof before fix | Result |
| --- | --- | --- |
| Abandon loses source restoration obligation | `TestReviewAbandonRestoresSourceOrKeepsCheckpoint`: HTTP 200 discarded state while source remained open, including failed closure and wrong identity | Restore original closed state with cluster/index UUID checks; retain checkpoint on failure |
| Selections cross cluster/direction/tab boundaries | UI tests retained old selections on Flip and tab switch, and allowed destination-panel selection | Clear on context changes; select only the migration source |
| Index named `constructor` cannot be selected | UI test found no own selection after clicking it | Selection dictionaries have no prototype |
| Reverse preview uses Node A list | UI test could not find selected `b-001` in preview options | Use the selected source side in either direction |
| Reverse history displays A → B for a B → A copy | UI test failed to find the actual host direction | History host arrow follows the saved direction |
| Offline destination hides healthy source | `TestReviewClustersRemainIndependent` returned HTTP 502; UI also cleared healthy source | Independent connection results and source-only browsing |
| Invalid counts accepted as successful | `TestReviewCountRejectsInvalidResults`: 403/missing count returned zero; negative and partial counts accepted | Reject HTTP errors and invalid/partial counts |
| Invalid disk metrics reported as valid | `TestReviewDiskRejectsInvalidResults`: 403/missing fields reported 0% used; partial results accepted | Reject HTTP errors, missing/invalid capacity and failed-node results |
| Configured watermarks ignored | `TestReviewDiskUsesEffectiveWatermarks`: returned 85%/90% instead of configured 70%/80% | Resolve transient, persistent, then default settings, including flat and nested forms |

Validation passed: the full Go race suite, `go vet`, 29 JavaScript regressions,
real-browser selection/preview/offline-cluster checks, and all 11 real Elasticsearch
8.17 migration scenarios using disposable local indices. The binary was rebuilt.
This review does not certify production safety or replace production-scale load,
snapshot-restore, or dependency-vulnerability testing. Migration buffers still use
document-count batches without a byte cap. Production clusters were not modified.

---

Production safety follow-up — 2026-09-28

The current changes address the twelve findings from the preceding review.
This is a code and regression-test review, not production certification. No
configured Elasticsearch cluster was contacted or modified during this work.

| Finding | Resolution | Regression evidence |
| --- | --- | --- |
| Ignored overwrite checkbox | Capture choice before removing modal; rename default, separate acknowledgement for overwrite | JavaScript choice, acknowledgement, identity, and local-prefix tests |
| Unsafe HTTP retries | Disable transport retries, including scroll and native submission | Actual production-client tests simulate lost 502 responses |
| Delete before replacement | Confirmed overwrite deletes only after staging is independently verified; rename preserves the original | Creation/bulk/corruption/health failures and stale UUIDs prevent deletion; uncertain delete/clone cannot replay |
| Abandon forgets native tasks | Reject saved/uncertain task abandonment | Both forms retain the active checkpoint |
| Partial native fallback | New native submissions disabled; failed saved tasks restart into a fresh copy | Recovery tests and preserved-partial-copy tests |
| Pipelines, lifecycle, aliases | Withhold aliases/lifecycle; disable pipelines; reject unsafe destination templates/components | Creation-body and template rejection tests |
| No independent verification | Write-block, refresh, read both sides; compare counts and fingerprints of IDs/routing/JSON contents | Corruption, numeric precision, routing, and completed-checkpoint revalidation tests |
| Concurrent destructive actions | Serialize mutations/previews; block index actions while migration remains unresolved | Action rejection tests and race-enabled suite |
| Credentials follow changed host | Construct a credential-free configuration for a new endpoint | Capturing mock receives no Authorization header |
| Unverified remote source | New copies always use the configured authenticated source client | Remote override cannot cause native submission |
| Relative state and multiple processes | Config-directory state; Unix user-wide and checkpoint-file locks | Stable-path and lock-exclusion tests |
| Misleading UI identity/results | Backend identities, pinned confirmation UUIDs, per-item failure reporting | Browser logic and stale-confirmation API tests |

Additional issues addressed during the second pass:

- Source UUIDs are captured when indices are queued and checked again on recovery.
- By default, sources must already be write-blocked. Optional live copies have
  limited verification as described below; transfers are not live replication.
- Closed-index previews persist their restoration obligation before opening;
  restart checks cluster/index identities before closing anything.
- Completed-index revalidation restores the checkpointed closed states even after
  interruption of an earlier verification attempt.
- Index operations reject unacknowledged responses, and mutation endpoints accept
  exact index names only.
- HTTP and HTTPS endpoints are accepted, including remote HTTP hosts (the
  HTTPS-only remote restriction was removed at the user's request on 2026-10-12).
  HTTP traffic and credentials are unencrypted. HTTPS certificate verification
  remains enforced; embedded URL credentials and mixed multi-endpoint pools
  are rejected.
- Large numbers in mappings/settings are decoded without float64 rounding.
- The unused whole-cluster count request was removed from cluster-info lookups.
- Development Docker ports now bind only to loopback.

Behavior changes requiring operational planning:

- Cross-cluster collisions prompt for a separate `<name>_reindex_<unique suffix>`
  copy (default) or an explicitly acknowledged overwrite. Overwrite removes the
  previous data, configuration, and aliases; filtered migrations replace the
  destination with only matching source documents.
- Overwrite builds and verifies staging before deleting the confirmed, write-blocked
  destination, then clones staging into the original name with writes blocked.
  Staging is retained. Replacement is not atomic and the original name may be
  unavailable if cloning fails. Green staging health and clone disk capacity are
  prerequisites. No automatic rollback or alias recreation is provided.
- Durable delete/clone submission markers prevent an ambiguous request from being
  reissued on Continue or forgotten through Abandon. These states require manual
  reconciliation. A confirmed-deleted checkpoint resumes cloning; finalizing
  checkpoints only verify and complete cleanup.
- Successful destinations are verified under a write block and closed, then
  match the source’s captured write-block state. Older completed checkpoints
  without this metadata retain their block. Reopening copies and application
  cutover are explicit operator actions.
- Ordinary same-cluster reindexing creates `reindex_<name>_<unique suffix>` and
  preserves the source. The explicit mapping workflow can remove the original
  after verified copying and the selected backup policy. Cluster UUIDs identify local copies even through different
  endpoints. Legacy API `overwrite: true` still requests a separate copy.
- Continue re-reads completed copies; this increases recovery I/O and runtime.
- Old completed checkpoints without verification metadata, and old native tasks
  without source identity, require manual reconciliation. Do not delete state to
  bypass an outstanding task.
- State lives beside the config file. Move an old checkpoint from its former
  working directory before upgrading if these locations differ.

Validation performed:

- Full Go suite with the race detector, including real child-process kill/restart
  tests against local mock clusters.
- Replacement admission, verification-before-delete, stale identity, reverse
  direction, local-prefix, pending-request, and finalization recovery tests.
- JavaScript regression tests executing the actual UI functions.
- Go vet, build to /tmp, and whitespace checks.
- Source review of destructive calls, completion transitions, transport retries,
  state persistence, identity checks, and restoration paths.
- On 2026-10-12, real local two-cluster integration suites passed on Elasticsearch
  7.17.16, 8.17.0, and 9.0.0. Each covered new destinations, rename, staged
  overwrite/clone, same-cluster copies, and local/cross-cluster mapping changes
  with filtered destinations and full original backups. Each case reloaded a
  saved checkpoint and used Continue to verify completed data and restore closed
  states. Tests used unique disposable indices and security-disabled containers.
- Matching-major admission now permits 7.14+ to 7.14+, 8.x to 8.x, and 9.x to 9.x;
  cross-major and malformed/prerelease versions are rejected before mutation,
  including on Continue. Header selection is tested with client compatibility
  mode both enabled and disabled in the environment. Production-specific mappings
  and settings still require successful creation and document verification.
- Real-server testing found and fixed acknowledged no-op handling for existing
  write blocks and closed indices, plus copying the private Elasticsearch 9
  `index.verified_read_only` setting. Partial/unacknowledged responses remain
  failures; accepted no-ops require a follow-up state check.

Remaining release gates and limits:

- Optional live-copy mode (2026-10-13) retains source mappings/settings and
  temporarily opens closed sources, restoring their initial state on exit. It
  verifies destination readback against the document stream captured
  during copying and reports `copied_live`, not full current-source verification.
  Overwrite and source backup/removal are prohibited.
  Consent and captured fingerprints survive restart; Continue reads back the
  destination and restores originally closed sources. A hard process stop may
  leave a source open until restart/Continue; restoration failures are reported.
  The UI exposes the limitation in
  confirmation, review steps, jobs, and history. This is not live synchronization.
  Live-copy integration tests passed on Elasticsearch 7.17.16, 8.17.0, and 9.0.0,
  including restart/Continue and mapping changes. Mock tests also check that no
  source-index mutations other than authorized temporary open/close are issued.
  Closed-source live copies and mapping reindexes passed on local Elasticsearch
  7.17.16 and 8.17.0. Failure, UUID replacement, and process-kill restart tests
  cover restoring closed sources and surfacing restoration errors.

- Repeat the opt-in integration suite on disposable clusters matching the exact
  production versions (the local runs above do not cover every minor/patch pair),
  with actual authentication/TLS, mappings, templates, and lifecycle configuration.
  Authentication/TLS deployment and dependency vulnerability audits are not
  replaced by the mock tests.
- Test snapshot restoration, disk capacity, large-document behavior, throughput,
  and the extra I/O of full verification before production use. No throttling or
  live-write catch-up mechanism is provided.
- For full source/destination verification, keep source writes blocked and external index/lifecycle/template administration
  quiescent. Privileged external actors can change UUIDs, blocks, templates, or
  cluster routing between checks; this application cannot exclude those actors.
- Verification compares API-visible _source documents. Source credentials need
  unrestricted document/field visibility. This is not a complete-cluster backup,
  system-index/data-stream migration, or preservation of external cluster objects.
- Fingerprint equality provides cryptographic assurance, not an absolute proof
  against every storage, hardware, permission, or operational failure.

Within those explicit boundaries, the reviewed mutation/recovery paths no longer
contain the identified delete-before-copy, hidden-overwrite, unsafe automatic
retry, or unverified-completion behavior. Production readiness still requires
passing the real-cluster and operational gates above.

Mapping workflow follow-up:

- Per-index paste/upload input, editable destination/backup names, and a separate
  step-review screen capture an explicit removal acknowledgement. Keep-original
  is the default. Source and cluster UUIDs are pinned at confirmation.
- Custom mappings apply only when creating new destinations. Every supplied
  mapping option is checked on the created index before copy and before removal;
  Elasticsearch creation/bulk failures leave the original intact.
- Source-side backups independently verify every original document and the
  original mapping before deletion. Filters apply only to the remapped destination.
- Checkpoints retain mapping JSON without rounding large numeric parameters,
  backup identities/fingerprints, and the original deletion phase. Ambiguous
  original deletion blocks Continue and Abandon. Completed verification after
  removal uses saved destination/backup evidence and restores closed states.
- Mock/race tests cover all retention choices locally and across clusters, reverse
  direction, filtered backup completeness, unsafe plans, mapping/copy/backup
  failures, partial-backup recovery, post-deletion recovery, and changed copies.
  JavaScript tests cover upload/paste, editable names, review/back, acknowledgements,
  pinned identities, and invalid/oversized inputs. These are not real ES validation.
- Backups preserve documents/mappings, not every index/cluster object. They consume
  additional storage and I/O and do not replace a tested Elasticsearch snapshot.
  External management must remain quiescent throughout the operation. Custom
  analysis settings are not accepted by the mapping editor.
