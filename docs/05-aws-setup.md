# AWS setup and runbooks

Region **`eu-north-1` (Stockholm)**. The design and costs are in ADR-0009. Run the commands
from the repository root in Git Bash or PowerShell. Open a new terminal after installing the
tools, so `aws` and `terraform` are on PATH.

## 1. Account (once, by the owner)

The account was created with AWS's **new sign-up experience**, on the **Free plan** ($100 of
credits plus up to $100 earned, 6 months). That mode has fixed rules:

- **One region,** chosen from the contact address. Ireland gets `eu-north-1`, and it cannot be
  changed.
- **The account sits in an AWS-managed organization** whose service control policies allow the
  Free-tier services. Those include S3, Lambda, EventBridge, SNS, CloudWatch, Systems Manager,
  Athena, Glue, Lake Formation, ECS, EC2 and DynamoDB.
- **EMR and MSK need the Paid plan.** IAM Identity Center and Organizations are not available.
- **The Free plan never bills,** but the account closes after 6 months (17 May 2027) or when the
  credits run out, unless it is upgraded. Keep a copy of the archive on the laptop (§5) or
  upgrade before then.

Steps:

1. Sign up (Basic support). Add MFA to the root user and stop using root.
2. In IAM, create a user `nathan-admin` with **AdministratorAccess** attached directly.
   Console access could not be enabled for this user, so it has no password and needs no MFA.
3. On the user's *Security credentials* tab: **Create access key**, use case *Command Line
   Interface*. Load it into a CLI profile, then set `AWS_PROFILE=ghostbus` in `.env`. The key
   lives in `~/.aws/credentials`, outside the repository. A downloaded `*accessKeys*.csv` is
   git- and docker-ignored, but keep it out of synced folders.

```bash
aws configure --profile ghostbus
aws sts get-caller-identity --profile ghostbus
```

The second command should print `user/nathan-admin`.

If a call fails with `explicit deny in a service control policy`, the service or region is
outside the plan. Check it before writing Terraform for it:

```bash
aws iam simulate-principal-policy --policy-source-arn arn:aws:iam::<account>:user/nathan-admin   --action-names <service:Action>   --context-entries ContextKeyName=aws:RequestedRegion,ContextKeyValues=eu-north-1,ContextKeyType=string
```

## 2. Bootstrap (once): state bucket and budget alert

Copy `terraform.tfvars.example` to `terraform.tfvars` in `bootstrap` and `core` and set
`alert_email`. Both files are git-ignored.

```bash
export AWS_PROFILE=ghostbus
terraform -chdir=infra/terraform/bootstrap init
terraform -chdir=infra/terraform/bootstrap apply
terraform -chdir=infra/terraform/bootstrap output -raw backend_hcl > infra/terraform/core/backend.hcl
```

AWS Budgets emails you at $5, $10 and $20 actual spend in a month, and when the month's
forecast passes $20 (`-var monthly_budget_usd=...` to change).

## 3. Core: archive and collector, schedules off

```bash
uv run python infra/lambda/build_collector.py
uv run python infra/scripts/put_nta_key.py
terraform -chdir=infra/terraform/core init -backend-config=backend.hcl
terraform -chdir=infra/terraform/core apply
```

- The NTA key script reads the key from `.env`. It never prints it, and the key never enters
  Terraform state.
- Confirm the SNS subscription email that arrives.
- The static watcher does not use the rate-limited GTFS-R API, so it can be tested at once:

```bash
aws lambda invoke --function-name ghostbus-static-watcher out.json && cat out.json
```

## 4. Cutover: the poller moves from the laptop to AWS

**Only one poller may use the key** (ADR-0005). Do these in order, without long pauses: every
minute between steps 1 and 3 is a minute of `NO_DATA`.

1. Stop the laptop collector:

```bash
docker compose -f local/docker-compose.yml stop poller static-watcher
```

2. Copy the laptop archive up. The heartbeats tell the Lambda when the laptop last called.

```bash
aws s3 sync ~/ghostbus-archive s3://$(terraform -chdir=infra/terraform/core output -raw archive_bucket)/ --exclude "*/.tmp-*"
```

3. Turn the schedules on. Also set `collector_enabled = true` in `core/terraform.tfvars`, so a
   later plain `apply` keeps them on:

```bash
terraform -chdir=infra/terraform/core apply -var collector_enabled=true
```

4. Watch the first calls:

```bash
aws logs tail /aws/lambda/ghostbus-poller --follow
```

Expect one `"called": true` line per minute and feeds alternating as on the laptop.

**Never start the laptop `poller` again** while the schedule is enabled. To move back, disable
the schedule (`collector_enabled=false`) first.

## 5. Working locally with cloud data

The laptop Spark jobs read `~/ghostbus-archive`. Pull new data down before `reconcile`:

```bash
aws s3 sync s3://$(terraform -chdir=infra/terraform/core output -raw archive_bucket)/raw ~/ghostbus-archive/raw
```

## 6. Park or destroy

- **Pause collection:** `collector_enabled=false`. Nothing else in `core` costs money when idle
  except storage.
- **Destroy everything except the data:** the archive and state buckets have `prevent_destroy`,
  so `terraform destroy` in `core` stops before deleting the archive. That is deliberate. Removing
  the archive is a manual, separate decision.
