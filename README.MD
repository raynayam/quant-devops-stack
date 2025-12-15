Production-ready containerized stack for backtesting and live trading quantitative strategies.

## Architecture
Remote (You) --> [VPN:10.8.0.1] --> Internet --> [Firewall:Port 1194] --> Docker Host:192.168.1.1
                                                                                        |
                                                                        .---------------'--------------.
                                                                        |       Docker Bridge          |
                                                                        |       172.18.0.0/16          |
                                                                        '------------------------------'
                                                                                |
                                                                        .------------------------------------------------------.
                                                                        | [Jupyter:8888] <--> [Prometheus:9090] --> [Grafana:3000] |
                                                                        |     |                  |                   ^            |
                                                                        |     v                  v                   |            |
                                                                        | [MLflow:5000] <--> [Redis:6379]       [Loki:3100]    |
                                                                        |     |                  |                              |
                                                                        |     v                  v                              |
                                                                        | [MinIO:9000] <--> [TimescaleDB:5432]                 |
                                                                        '------------------------------------------------------'
- **JupyterLab**: Strategy development (port 8888)
- **TimescaleDB**: Market data storage (port 5432)
- **MLflow**: Model tracking (port 5000)
- **MinIO**: Artifact storage (port 9000)
- **Redis**: Signal cache (port 6379)
- **Prometheus + Grafana + Loki**: Observability (ports 9090, 3000, 3100)

## Quick Start

```bash
# 1. Clone
git clone https://github.com/raynayam/quant-devops-stack
cd algorithmic-trading-platform

# 2. Configure
cp .env.example .env
# Edit .env with your credentials

# 3. Start
docker-compose up -d
