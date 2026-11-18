# Security: threat model, controls and evidence

**Written:** 18 November 2026. Diagrams and interview Q&A: [`docs/diagrams/08-security.md`](diagrams/08-security.md).

Ghost Bus Radar holds no customer data and takes no payments. So the security question is
mostly about **integrity**: if the project says "Route X ghosted 8% of its trips", nobody, not an
attacker, not a bug, not the author, should be able to have changed the data behind that number
without it showing. The other questions are the usual ones for a cloud account: a leaked key, a
runaway bill, a compromised dependency.

## 1. What we protect

| # | Asset | Why it matters | Worst case |
|---|---|---|---|
| A1 | NTA API key | Issued to the owner under NTA's usage policy | Misuse gets the key revoked; collection stops |
| A2 | The raw archive (S3) | System of record; every number is derived from it | Silent edits or deletions make published numbers unverifiable |
| A3 | Published numbers (gold, emails, future website) | The project's whole value is that they can be trusted | A wrong or manipulated rate is published as fact |
| A4 | AWS account and credentials | Root, the `nathan-admin` CLI key, service roles | Account takeover: data loss, crypto-mining bill |
| A5 | Cost | The account must stay cheap | Runaway spend |
| A6 | Personal data | Field-check observations (where someone stood, when), the owner's email | Privacy harm |
| A7 | Code and build pipeline | What runs in Lambda and Glue | A malicious dependency or CI action runs with deploy rights |

## 2. Trust boundaries

1. **NTA → our poller.** The data comes from NTA over HTTPS. We trust NTA to be NTA, not to be
   correct; every number is about what NTA's systems reported.
2. **Laptop → AWS.** The owner's CLI key crosses here; so do Terraform deploys and archive syncs.
3. **Service → service inside AWS.** Each component has its own IAM role.
4. **GitHub → anyone.** The repository is public; CI runs third-party actions and packages.
5. **AWS-managed organization → our account.** AWS's service control policies cap every identity
   in the account, admins included (region, services).

## 3. Threats (STRIDE) and controls

| Threat | Example here | Controls | Status |
|---|---|---|---|
| **Spoofing** | Someone uses a stolen CLI key | Key only in `~/.aws/credentials`; CloudTrail logs every call; tripwires email on sensitive actions; 90-day rotation | done; tripwires new |
| | A fake "NTA" response | HTTPS with certificate checks (httpx default); protobuf parsed strictly; bad payloads archived but marked `bad_payload` | done |
| **Tampering** | Raw data edited or deleted after the fact | Bucket policy denies `DeleteObject` under `raw/` to everyone; versioning keeps overwritten bytes; **hash-chained daily manifest** proves the archive unchanged since the night it was sealed | deny + versioning done; **chain new** |
| | Audit trail switched off | CloudTrail log-file validation; **tripwire on StopLogging / DeleteTrail / UpdateTrail / PutEventSelectors** | validation done; **tripwire new** |
| | A bucket policy loosened | **Tripwire on PutBucketPolicy / DeleteBucketPolicy / PutBucketVersioning** on our buckets | **new** |
| | Malicious code via a dependency or CI action | Lock file with exact versions; **CI: pip-audit (known CVEs), gitleaks (secrets), checkov (Terraform), actions pinned to commit SHAs, SBOM**; reproducible Lambda zip | lock + reproducible done; **CI checks new** |
| **Repudiation** | "I didn't change that" | CloudTrail with validated log files, kept a year; every deploy is a Git commit and a Terraform plan | done |
| **Information disclosure** | NTA key ends up in logs, heartbeats, Git, Terraform state, chat | Key only in `.env` (ignored) and SSM SecureString; Terraform knows only its name; **redaction of the key from every stored error, with a test that feeds the key into an error**; gitleaks in CI | SSM done; **redaction + gitleaks new** |
| | Someone other than the poller reads the key | Only the poller role may `GetParameter` it; **tripwire on any other reader** | **tripwire new** |
| | Field-check data | Git- and Docker-ignored; never uploaded | done |
| **Denial of service** | Our own bug calls NTA too often and gets the key revoked | One call per minute by construction (pure pacing function, S3 conditional-write minute claim, property tests); no retries inside a minute | done |
| | Runaway cost | $20 budget alerts; Free plan cannot bill; Flex Glue; Athena 1 GB cap; schedules behind flags | done |
| **Elevation of privilege** | A service role used for more than its job | One role per component, scoped to its own resources; Access Analyzer validates every policy (0 findings); **Glue moves off the broad AWS-managed `AWSGlueServiceRole` to a least-privilege policy** | roles done; **Glue policy new** |
| | Admin key without MFA | Accepted risk with compensating controls (below); SCPs cap the blast radius | documented |

### The integrity threat specific to this project

The most damaging attack on a public statistic is not stealing it but **quietly changing it**: delete
the morning snapshots of an embarrassing day, or replace them, and the published rate changes with
no trace. Three layers stop that:

1. **Prevent:** nobody can delete under `raw/` (bucket policy, admins included).
2. **Detect:** every night a manifest of the previous day's archive objects (key, size, ETag) is
   hashed together with the previous day's manifest hash. Changing any object of any past day
   changes every later hash. The chain head appears in the daily email, so a copy lives outside
   AWS. `verify_provenance` recomputes the chain from S3 and names the first day that differs.
3. **Attribute:** CloudTrail shows who changed a bucket policy to make an edit possible, and the
   tripwire has already emailed about it.

The same honesty principle drives the outcome rules (ADR-0008): our own downtime can never be
published as a ghost. Integrity of the numbers is treated as a security property, not only a data
quality one.

## 4. Accepted risks (and why)

| Risk | Why accepted | Compensating controls |
|---|---|---|
| The `nathan-admin` CLI key has no MFA | The account type cannot give that user console access or MFA | Key never in synced folders or repo; CloudTrail; tripwires; budget alerts; SCP region and service caps; rotation every 90 days; deploys move to GitHub OIDC (roadmap 4.3) |
| The downloaded key CSV is in a OneDrive-synced folder (owner's choice) | Owner decision | Git- and Docker-ignored; rotation removes its value |
| Single region, single bucket | Multi-region is not allowed on this plan | Versioning, deny-delete, hash chain, laptop copy of the archive |
| IAM and console-login events are not alertable | Global services log to `us-east-1`, which the account's SCPs deny | Owner sign-in through AWS Builder ID with MFA; root has no password; CloudTrail event history covers 90 days |

## 5. Evidence (all on 18 November 2026)

| Control | How it was verified |
|---|---|
| TLS-only buckets | A plain-HTTP `ListObjectsV2` was refused |
| No deletes under `raw/` | An admin `DeleteObject` was refused; deletes elsewhere still worked |
| Least privilege | IAM Access Analyzer `validate-policy`: 0 findings on every role policy and bucket policy, including the new Glue policy that replaced `AWSGlueServiceRole`; a full Glue run then succeeded under it |
| Key never stored in errors | Unit test feeds the key into an exception and an NTA error page; the test fails with redaction removed |
| Hash chain | 8 unit tests (edit, add, delete, overwrite, broken link, property test); Glue sealed 15 to 17 Nov from genesis; the laptop recomputed the chain from S3 independently and matched (`00e259e5`, `ee119ce4`, `796fb0ad`) |
| Tripwires | Each fired by a real action: reading the key's metadata (secret-read 1), re-applying the trail's selectors (audit-trail 1), re-applying a bucket policy and enabling versioning (bucket-guardrails 2); EventBridge `TriggeredRules` and SNS deliveries confirmed |
| Supply chain | gitleaks over the full history (1 false positive, fingerprinted); pip-audit (1 advisory, triaged below); checkov 231 passed, 0 failed (skips justified below); every action pinned to a commit SHA |

## 6. Owner actions

1. **Owner sign-in: done, and correctly understood.** The audit read `AccountMFAEnabled = 0` as
   "root has no MFA". It was the wrong lens for this account type. The IAM credential report shows
   the root user has **no password at all**. The account is owned by an **AWS Builder ID**, which
   signs in with Google and has an authenticator-app MFA (TOTP), registered 17 November. So the
   owner login has two factors. The remaining chain is that Google account: it must have 2-Step
   Verification, as must the recovery address.
   *Lesson:* an audit signal is only as good as its model of the system. "Root MFA off" is a
   finding on a classic account; on a Builder-ID-owned one, the control lives elsewhere.
2. Rotate the CLI key by 15 February 2027 (`docs/05-aws-setup.md` section 7).
3. In GitHub settings, enable Dependabot alerts and private vulnerability reporting.

## 7. Scanner triage

**pip-audit, `PYSEC-2026-3966` / CVE-2026-32773 (PySpark 3.5.6, fixed in 3.5.8).** Cross-site
scripting in the Spark History Server. Exploiting it needs the ability to launch Spark jobs and a
more privileged user opening the History Server. No History Server runs anywhere in this project
(local Spark UIs are bound to localhost; Glue's Spark is managed by AWS), and only the owner
launches jobs. Upgrading the local PySpark would break parity with Glue 5.1, which ships Spark
3.5.6. **Accepted**; suppressed in CI by this ID only, so any new advisory fails the build.
Re-check by 15 February 2027, or when Glue ships Spark 3.5.8.

**gitleaks, `generic-api-key` in `publisher.py` (commit `0d29ebc`).** The match is `key=key`, the
Kafka message-key argument. **False positive**, ignored by its exact fingerprint in
`.gitleaksignore`.

**checkov in CI vs on the laptop.** The first CI run failed `CKV_AWS_319` (alarm actions enabled)
although the laptop passed: the laptop scan read the git-ignored `terraform.tfvars`
(`collector_enabled = true`) and CI scanned the defaults. Skipped with the reason: alarm actions
follow the collector they watch. Lesson: a scanner's verdict depends on the variable values it sees.

**checkov.** 4 findings fixed (abort incomplete uploads on two buckets, versioning on the audit-log
bucket, a Glue security configuration). 17 rules skipped, each with its reason in `.checkov.yaml`.
They fall into three groups:
- forbidden by the account's SCPs: multi-region trail, cross-region replication;
- deliberate design: no VPC for the Lambda, no customer KMS keys, 14-day application logs;
- planned: Lambda code signing.

One is a trap worth knowing: KMS-encrypting the SNS topic with the AWS-managed key would **stop
every alert**, because EventBridge and CloudWatch cannot use that key.

## 8. Next steps

- S3 Object Lock (governance mode) on `raw/`: prevention to match the chain's detection.
- S3 SHA-256 object checksums in the manifest instead of ETags.
- Lambda code signing with AWS Signer.
- Deploy from CI with GitHub OIDC, then delete the CLI access key (roadmap 4.3).
- On a Paid plan: GuardDuty, and CloudTrail data events for the archive.

