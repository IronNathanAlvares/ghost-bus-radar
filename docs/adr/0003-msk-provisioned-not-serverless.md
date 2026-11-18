# ADR-0003: MSK provisioned (small brokers), not MSK Serverless

**Status:** accepted, 15 November 2026. Prices to be confirmed against the AWS pricing page in S5.

## Context

Throughput is tiny: two feeds polled every 60 s, roughly a few MB a minute. MSK Serverless bills
about $0.75 an hour per cluster before partitions and data, roughly $550 a month. Two
`kafka.t3.small` brokers cost roughly $70 a month. Both support IAM authentication; MSK supports
Kafka 4.x in KRaft mode.

## Decision

MSK provisioned, 2 x `kafka.t3.small`, Kafka 4.1 (matching the local `apache/kafka:4.1.1`),
IAM auth, in private subnets. The cluster exists only during showcase windows (`PLAN.md`
section 9); ADR-0001 makes that safe.

## Consequences

- Roughly 8x cheaper for this workload.
- We own broker sizing, which is trivial at this volume.
- t3 brokers have burst credits; sustained replay of weeks of archive must be rate-limited.
- Upgrading to Kafka 4.2 on MSK is an in-place rolling update if needed later.
