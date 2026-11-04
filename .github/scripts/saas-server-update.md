# Landed server updates

`update-saas-server.yml` is a main-push trigger for `temporalio/temporal`. It stays disabled unless the repository variable `SAAS_SERVER_UPDATES_ENABLED` is exactly `true`. The script also checks this switch before dispatching; unset and false values send no requests.

The full-history checkout supplies the immutable push `before..after` range. A squash merge contributes its final landed SHA, excluding the PR's pre-squash commits. A push containing several landed commits contributes each SHA. Missing, null, unavailable, backward or divergent ranges fail visibly without substituting the tip. Non-main, unmerged and fork-repository events are excluded.

Each candidate is passed in full as `server_commit` to SaaS `build-server-next-branch.yml` on main. The Actions summary lists accepted and unconfirmed dispatches; a partial error fails the job while retaining known accepted SHAs. An accepted request establishes only API acceptance. Receiving-run inputs, validation and actual merge require their own evidence. Event reruns repeat the same full candidate identities; SaaS publication must coordinate safe repeats. There is no replay loop or missing-run monitor.

The pinned App-token action requests Actions write and Contents read for `saas-temporal` using the existing `TEMPORAL_CICD_APP_ID` / `TEMPORAL_CICD_PRIVATE_KEY` configuration. Installed target grants remain unverified. Secret names or existing access to `api` do not establish SaaS workflow authority. Confirm the installation, target repository and required grants before activation. [Pinned action inputs](https://github.com/actions/create-github-app-token/blob/v2.2.2/action.yml).

Before separately authorized activation, land and verify required SaaS root/Walker ancestry protection and safe publication, then select the daily-updater setting, alert policy/destinations, OSS fallback and monitoring/responder handoff. Those values remain proposed configuration. Under a separately authorized live trial, verify that receiving run inputs/logs contain the exact landed SHA; do not infer this from dispatch acceptance. No dispatch is required for source validation.

Rollback disables `SAAS_SERVER_UPDATES_ENABLED`, then inspects outstanding SaaS PRs and queue enrollment manually. Keep required forward-only protection while any pending, daily or manual writer can update the pin. A still-enrolled PR can outlive its waiter; daily execution does not guarantee recovery. Automatic recovery/re-enqueue and additional interruption notifications remain deferred.

Offline fixtures use real Git histories and a stubbed `gh`:

```sh
python3 .github/scripts/saas-server-update_test.py
```
