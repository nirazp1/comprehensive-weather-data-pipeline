# Project Structure

This project is organized into logical folders for better maintainability:

## 📁 Directory Structure

```
.
├── src/                    # Core pipeline source code
│   ├── config.py          # Configuration management
│   ├── normalize.py       # Data normalization
│   ├── fetch_openweather.py
│   ├── fetch_many.py
│   ├── simple_end_to_end.py
│   ├── storage.py
│   ├── utils.py
│   └── data_generator.py
│
├── docker/                 # Docker configuration
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── .dockerignore
│   └── start_docker.sh
│
├── kafka/                  # Kafka-related code
│   ├── kafka_producer_example.py
│   ├── kafka_consumer.py
│   ├── simple_end_to_end_kafka.py
│   └── test_kafka.py
│
├── web/                    # Web interface
│   └── streamlit_app.py
│
├── scripts/                # CLI scripts
│   ├── generate_big_data_cli.py
│   └── run_streamlit.sh
│
├── examples/               # Example code
│   ├── playwright_example.py
│   └── airflow_dag_example.py
│
├── tests/                  # Test suite
│   ├── test_normalize.py
│   └── test_utils.py
│
├── docs/                   # Documentation
│   ├── README.md
│   ├── RESEARCH_PAPER.tex
│   └── about/
│
└── data/                   # Data directories
    ├── raw/
    └── processed/
```

## 🚀 Quick Commands

```bash
# Run pipeline
python3 -m src.simple_end_to_end

# Generate big data
python3 scripts/generate_big_data_cli.py --cities 25

# Run web app
streamlit run web/streamlit_app.py

# Docker
cd docker && docker-compose up -d
```

