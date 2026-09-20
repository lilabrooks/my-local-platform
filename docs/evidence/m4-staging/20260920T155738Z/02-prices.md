# M4 price check

Checked at: `2026-09-20T15:45:16Z`

Checked by: `Codex`

Region: `us-east-1`

| Item | Rate | Quantity | Hourly USD | Source |
|---|---:|---:|---:|---|
| MSK Serverless cluster | $0.7500000000/cluster-hour | 1 | $0.750000 | [AWS](https://aws.amazon.com/msk/pricing/) |
| MSK partitions | $0.0015000000/partition-hour | 13 | $0.019500 | [AWS](https://aws.amazon.com/msk/pricing/) |
| EKS standard-support control plane | $0.1000000000/cluster-hour | 1 | $0.100000 | [AWS](https://aws.amazon.com/eks/pricing/) |
| t3.medium on-demand upper bound | $0.0416000000/instance-hour | 2 | $0.083200 | [AWS](https://aws.amazon.com/ec2/pricing/on-demand/) |
| NAT gateway | $0.0450000000/gateway-hour | 1 | $0.045000 | [AWS](https://aws.amazon.com/vpc/pricing/) |
| NAT public IPv4 address | $0.0050000000/address-hour | 1 | $0.005000 | [AWS](https://aws.amazon.com/vpc/pricing/) |
| RDS db.t4g.micro | $0.0160000000/instance-hour | 1 | $0.016000 | [AWS](https://aws.amazon.com/rds/postgresql/pricing/) |
| RDS gp3 storage | $0.1150000000/GB-month | 20 / 730 hours | $0.003151 | [AWS](https://aws.amazon.com/rds/postgresql/pricing/) |

Total: **$1.0219/hour**

Gate: **PASS**, below the $1.25/hour limit.
