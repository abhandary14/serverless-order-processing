# Fault-Tolerant Order Processing Pipeline — Project Decisions

This document is the source of truth for architectural decisions and the reasoning behind them. Refer to this before making any design change — several decisions here resolve specific bugs identified during planning, and changing them without understanding why will reintroduce those bugs.

## Goal

Serverless order processing pipeline on AWS, demonstrating distributed systems fault tolerance and observability. Built to be defensible in technical interviews (Amazon SDE1 2026, cloud/infra track) — every component must be something the builder can explain and justify unassisted, not just describe.

## Scope (locked)

Matches these resume bullets exactly — do not add or remove components without updating the resume:

1. Built a serverless order pipeline with three AWS Lambda functions connected through SQS and simulated failures mid-order to confirm no orders were lost.
2. Added a dead-letter queue to catch orders that failed repeatedly while using retries with backoff through tenacity to recover from short-lived failures first.
3. Tracked queue depth, latency, and error rate with CloudWatch metrics and dashboards to catch and diagnose failures faster during testing.
4. Set up CI/CD with GitHub Actions to test and deploy each function on every push.

Explicitly cut from the original broader plan:
- Separate scheduled DLQ-checker Lambda (redundant with DLQ-triggered Lambda + alarm)
- More than 3–4 custom CloudWatch metrics
- SES/SNS real email delivery (log the confirmation instead)

## Architecture

Three Lambda functions:

1. **Order Intake** — HTTP endpoint (Function URL), validates payload, writes order to DynamoDB with status `received`, publishes message to SQS.
2. **Order Processing** — SQS-triggered. Checks inventory, runs mock payment, updates order status. Contains the idempotency logic (see below).
3. **Notification** — SQS-triggered (kept consistent with resume bullet 1, which states "connected through SQS" for all three functions). Sends mock confirmation (logged), updates final status.

**Note on notification trigger:** DynamoDB Streams was considered and is architecturally stronger (avoids a non-atomic second write/publish step). Rejected only because the resume bullet already commits to "connected through SQS" for all three functions, and changing it now means rewriting a submitted resume. If asked in an interview why SQS over Streams here, the honest answer is: Streams removes a failure mode (status updated but notification never published) at the cost of a less explicit pipeline; SQS was chosen for explicit, inspectable message flow across all three stages.

## Retry strategy — two layers, explicitly scoped to not collide

**Problem this solves:** tenacity retrying inside a Lambda invocation and SQS's visibility-timeout-based redelivery can otherwise overlap, causing the same message to be processed by two concurrent invocations at once.

**Resolution:**
- Tenacity: fast, in-process retries only. 2–3 attempts, total retry window a few seconds, for transient failures (mock payment timeout, DynamoDB throttling).
- SQS visibility timeout: set well above tenacity's total retry window (e.g., tenacity maxes at ~10s, visibility timeout = 60s), so tenacity always finishes before SQS could redeliver.
- SQS redrive policy (`maxReceiveCount`) owns the "give up, send to DLQ" decision — not tenacity. Tenacity never decides to give up permanently; it only handles short-lived blips within one invocation.

## Idempotency — two separate protections for two separate bugs

**Problem this solves:** message redelivery could otherwise (a) double-charge the mock payment, or (b) run the processing logic twice concurrently. These are different bugs requiring different fixes.

1. **Double-payment protection:** mock payment call uses `order_id` as an idempotency key. The moment payment succeeds, a `payment_id` is written to the order record immediately — before any other step. On any (re)delivery, processing checks first: does this order already have a `payment_id`? If yes, skip payment, go straight to finalizing status.
2. **Double-processing protection:** DynamoDB conditional write flips order status from `received` to `processing` at the very start of the function, conditioned on current status still being `received`. If two invocations race, the losing write fails the condition and backs off.

## DynamoDB schema

Two tables, not single-table design. Single-table design is the AWS-recommended pattern at scale, but adds modeling complexity not justified at this project's size — two simple tables are easier to reason about and explain without over-explaining DynamoDB theory in an interview.

**`Orders` table**
- Partition key: `order_id` (string, UUID)
- Attributes: `item_id`, `quantity`, `customer_email`, `status`, `payment_id`, `created_at`, `updated_at`
- GSI1: partition key `status`, sort key `created_at` — supports querying orders by status (e.g., all `failed` orders), pre-sorted by time
- GSI2: partition key `customer_email`, sort key `created_at` — supports querying orders by customer, pre-sorted by time

**`Inventory` table**
- Partition key: `item_id` (string)
- Attributes: `stock_level`

**Capacity mode: Provisioned, not on-demand.** AWS's Always Free tier (25 RCU / 25 WCU account-wide) applies specifically to provisioned mode. On-demand is low-cost but not guaranteed $0, which conflicts with the stated always-free constraint.

**Capacity allocation:**

| Resource | RCU | WCU |
|---|---|---|
| `Orders` table | 3 | 3 |
| `Inventory` table | 3 | 3 |
| `Orders` GSI1 (status) | 1 | 1 |
| `Orders` GSI2 (email) | 1 | 1 |
| **Total** | **8** | **8** |

Leaves 17/25 headroom under the account-wide free tier ceiling for future projects. Base tables get more capacity than GSIs since they absorb the actual write traffic during the chaos test; GSIs are read-light for this use case.

**Note:** provisioned capacity is billed hourly for the allocation whether used or not (unlike Lambda/SQS, which are pure pay-per-request with zero idle cost). Staying at or under 25/25 total account-wide keeps this at $0 regardless of how long the stack stays deployed.

## Dead-letter queue

DLQ-triggered Lambda (not a scheduled poller) — reuses the same event-driven pattern already in use elsewhere in the stack, fires immediately on arrival rather than on a polling delay. Backed by a CloudWatch alarm on DLQ depth > 0 as a redundant safety net.

## Observability

- Structured JSON logging in every function: order id, function name, outcome on every log line.
- Custom CloudWatch metrics (3–4, not more): orders received, orders failed, DLQ depth, and optionally processing latency.
- One CloudWatch dashboard.
- One alarm: DLQ depth > 0.

## Chaos test — the centerpiece deliverable

Resume claims "simulated failures mid-order to confirm no orders were lost." The test must verify three separate outcomes, not one:

1. No orders lost (transient failures recover via tenacity retry)
2. No orders stuck silently (persistent failures land in DLQ, not vanish)
3. No orders double-processed (the idempotency protections above actually hold under redelivery)

Test plan: send 50 orders, inject failures at a known rate into the mock payment step, verify all three outcomes against DynamoDB records post-test. This test's output (logs, record states, dashboard screenshot) must be captured into the README, since it's the artifact that needs to survive even if infrastructure is later torn down.

## Infrastructure as code

**AWS SAM**, not Serverless Framework. Reasoning: AWS-native (better signal for an AWS-focused application), no third-party licensing risk (Serverless Framework has gated some features behind a paid tier), built-in local testing via `sam local invoke` without needing plugins.

## Runtime

Python 3.14 (`Runtime: python3.14` in template). Matches local dev environment exactly, avoiding version mismatch between local testing and deployed Lambda. Verified `boto3` and `tenacity` install cleanly on 3.14 before committing to it over the more established 3.12/3.13.

## AWS account setup

- All work done under a dedicated IAM user (`sam-deploy-user`) with programmatic + console access — never root, except for account-level actions (billing, MFA, creating the IAM user itself).
- Root account has MFA enabled.
- IAM user policies: `AWSLambda_FullAccess`, `AmazonSQSFullAccess`, `AmazonDynamoDBFullAccess`, `CloudWatchFullAccess`, `AWSCloudFormationFullAccess` (required — SAM deploys via CloudFormation), `IAMFullAccess` (temporary, intended to be tightened to scoped per-function execution roles once the stack exists — flagged as a deliberate follow-up, not an oversight).
- Billing alarm set at $1 threshold.
- Long-lived access keys used (not IAM Identity Center SSO) — a deliberate, scoped trade-off given time constraints, not a default. Access key will be deleted when the project is no longer actively worked on.

## CI/CD

GitHub Actions: run unit tests + Ruff linting on every push, deploy via SAM on merge to main. Unit tests mock AWS SDK calls — no tests hit real AWS.

## Explicit non-goals

- No EC2, RDS, or S3 usage without explicit sign-off (these draw from the $200 credit balance, not the always-free tier).
- No SES/SNS — notification is logged only.
- No single-table DynamoDB design.
- No IAM policy hardening beyond the initial broad grant until the stack exists and per-function roles can be scoped deliberately.