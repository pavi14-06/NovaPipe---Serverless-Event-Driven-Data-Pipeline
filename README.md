# 🚀 NovaPipe — Serverless Event-Driven Data Pipeline

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-blue?style=for-the-badge&logo=python"/>
  <img src="https://img.shields.io/badge/AWS_Lambda-Serverless-orange?style=for-the-badge&logo=amazonaws"/>
  <img src="https://img.shields.io/badge/Apache_Kafka-Event_Streaming-black?style=for-the-badge&logo=apachekafka"/>
  <img src="https://img.shields.io/badge/Cassandra-NoSQL-1287B1?style=for-the-badge&logo=apachecassandra"/>
  <img src="https://img.shields.io/badge/Terraform-IaC-7B42BC?style=for-the-badge&logo=terraform"/>
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker"/>
</p>

> A **production-grade**, fully automated **serverless event-driven data pipeline** that ingests, transforms, and stores high-velocity data streams using **AWS Lambda**, **Apache Kafka**, **Cassandra**, and **S3** — with Bash automation orchestrating the entire lifecycle.

---

## 🏗️ Architecture Overview

```
                          ┌─────────────────────────────────────────────────────┐
                          │                   NovaPipe Architecture              │
                          └─────────────────────────────────────────────────────┘

  [Event Sources]        [Streaming Layer]     [Compute Layer]      [Storage Layer]
  ─────────────          ───────────────       ─────────────        ──────────────
  IoT Sensors   ──►                            
  REST APIs     ──►  Kafka Producer  ──►  Kafka Topic  ──►  Lambda Consumer  ──►  Cassandra (Hot)
  Webhooks      ──►  (Python)            (Partitioned)      (Python)          ──►  S3 Data Lake (Cold)
  Databases     ──►                            │
                                               ▼
                                       Lambda Transformer
                                       (Enrichment + Filter)
                                               │
                                      ┌────────┴────────┐
                                      ▼                 ▼
                               Dead Letter           Monitoring
                               Queue (SQS)          (CloudWatch)
```

---

## ✨ Features

| Feature | Description |
|---|---|
| 🔄 **Event Streaming** | High-throughput Kafka topics with partition-based parallelism |
| ⚡ **Serverless Compute** | AWS Lambda auto-scales to handle millions of events/sec |
| 🗄️ **Dual Storage** | Hot data in Cassandra, cold data archived to S3 |
| 🔁 **Auto Retry** | Dead Letter Queue with exponential backoff |
| 📊 **Observability** | CloudWatch metrics + custom dashboards |
| 🔒 **Secrets Management** | AWS Secrets Manager integration |
| 🐳 **Local Dev** | Full Docker Compose local environment |
| 🏗️ **IaC** | 100% Terraform-managed infrastructure |
| 🤖 **Automation** | Bash scripts for deploy, teardown, scaling |
| ✅ **Testing** | Unit + Integration tests with mocks |

---

## 📁 Project Structure

```
novapipe/
├── 📂 terraform/               # Infrastructure as Code
│   ├── main.tf                 # Root Terraform config
│   ├── variables.tf            # Input variables
│   ├── outputs.tf              # Output values
│   ├── lambda.tf               # Lambda function resources
│   ├── kafka.tf                # MSK Kafka cluster
│   ├── cassandra.tf            # Keyspaces (Cassandra)
│   └── s3.tf                   # S3 buckets
│
├── 📂 lambda/                  # AWS Lambda functions
│   ├── producer/               # Event producer Lambda
│   │   ├── handler.py          # Main handler
│   │   └── requirements.txt
│   ├── consumer/               # Kafka consumer Lambda
│   │   ├── handler.py          # Main handler
│   │   └── requirements.txt
│   └── transformer/            # Data transformation Lambda
│       ├── handler.py          # Main handler
│       └── requirements.txt
│
├── 📂 kafka/                   # Kafka configuration
│   ├── topics.py               # Topic management
│   └── config.py               # Kafka settings
│
├── 📂 cassandra/               # Cassandra setup
│   ├── schema.cql              # Keyspace & table definitions
│   └── client.py               # Cassandra client wrapper
│
├── 📂 s3/                      # S3 data lake logic
│   └── archiver.py             # S3 archival handler
│
├── 📂 docker/                  # Local dev environment
│   └── docker-compose.yml      # Kafka + Cassandra + Zookeeper
│
├── 📂 scripts/                 # Bash automation scripts
│   ├── deploy.sh               # Full deployment script
│   ├── teardown.sh             # Clean infrastructure teardown
│   ├── scale.sh                # Scale Lambda concurrency
│   ├── health_check.sh         # Pipeline health check
│   └── setup_local.sh          # Local dev environment setup
│
├── 📂 tests/                   # Test suite
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── test_transformer.py
│
├── 📂 docs/                    # Documentation
│   └── ARCHITECTURE.md
│
├── .env.example                # Environment variables template
├── Makefile                    # Developer shortcuts
└── README.md                   # You are here
```

---

## 🚀 Quick Start

### Prerequisites
- AWS CLI configured (`aws configure`)
- Terraform >= 1.5
- Docker & Docker Compose
- Python 3.11+

### 1️⃣ Clone & Setup
```bash
git clone https://github.com/YOUR_USERNAME/novapipe.git
cd novapipe
cp .env.example .env
# Edit .env with your AWS credentials and config
```

### 2️⃣ Start Local Environment
```bash
chmod +x scripts/setup_local.sh
./scripts/setup_local.sh
```

### 3️⃣ Deploy to AWS
```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

### 4️⃣ Health Check
```bash
./scripts/health_check.sh
```

### 5️⃣ Teardown
```bash
./scripts/teardown.sh
```

---

## ⚙️ Configuration

Copy `.env.example` to `.env` and fill in your values:

```env
AWS_REGION=us-east-1
AWS_ACCOUNT_ID=123456789012
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
CASSANDRA_CONTACT_POINTS=localhost
S3_BUCKET_NAME=novapipe-data-lake
PIPELINE_ENV=production
```

---

## 🧪 Running Tests

```bash
pip install -r lambda/consumer/requirements.txt
python -m pytest tests/ -v
```

---

## 📊 Pipeline Flow

1. **Producer Lambda** — Receives events from HTTP trigger / EventBridge schedule
2. **Kafka Topic** — Buffers and partitions events for parallel processing
3. **Transformer Lambda** — Enriches, filters, and validates incoming records
4. **Consumer Lambda** — Writes hot data to Cassandra, archives cold data to S3
5. **DLQ** — Failed events routed to SQS Dead Letter Queue with retry logic
6. **CloudWatch** — Metrics, alarms, and dashboards for full observability

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Streaming | Apache Kafka (AWS MSK) |
| Compute | AWS Lambda (Python 3.11) |
| Hot Storage | Apache Cassandra (AWS Keyspaces) |
| Cold Storage | AWS S3 |
| IaC | Terraform |
| Containerization | Docker & Docker Compose |
| Automation | Bash Shell Scripts |
| Monitoring | AWS CloudWatch |
| Secrets | AWS Secrets Manager |
| Queue | AWS SQS (Dead Letter Queue) |

---

## 📈 Performance

- Handles **100,000+ events/minute**
- Lambda cold start optimized with **provisioned concurrency**
- Kafka topics partitioned for **parallel processing**
- Cassandra schema optimized for **time-series write patterns**

---

## 🤝 Contributing

Pull requests are welcome! Please open an issue first to discuss what you'd like to change.

---

## 📄 License

MIT License — feel free to use this in your own projects!

---

<p align="center">Built with ❤️ using Python, Bash, AWS Lambda, Kafka & Cassandra</p>
