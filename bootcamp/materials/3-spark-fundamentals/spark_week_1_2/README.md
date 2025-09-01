# Spark Gaming Analytics

Migration of PostgreSQL queries from Weeks 1-2 to SparkSQL with optimizations.

## Structure
- `src/jobs/` - PySpark job implementations
- `src/tests/` - Test suites with fake data
- `src/utils/` - Shared utilities
- `config/` - Configuration files
- `data/sample/` - Sample input data
- `output/` - Job output results

## Setup
```bash
conda activate boot
pip install -r requirements.txt
```

## Run Jobs
```bash
python -m src.jobs.player_performance_analysis
python -m src.jobs.match_statistics_analysis
```

## Run Tests
```bash
pytest src/tests/ -v
```
