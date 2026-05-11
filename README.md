## MVP Distributed Load Balancer Simulator (E-Commerce Logs)

#### Github Link: https://github.com/leonistheczar/distributed_load_balancer

Python simulation that replays cleaned Zanbil.ir (Kaggle) HTTP requests through a pool of simulated backend nodes, comparing:

- **rr**: Round Robin
- **wrr**: Weighted Round Robin
- **lc**: Least Connections
- **ip_hash**: IP Hash (session affinity)

### Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -e . || pip install pandas numpy matplotlib seaborn rich pyyaml pytest hypothesis
```

### Get dataset (Kaggle)

Download Kaggle dataset `eliasdabbas/web-server-access-logs` and place:

- `access.log` → `data/raw/access.log`
- `client_hostname.csv` → `data/raw/client_hostname.csv` (optional)

### Build cleaned CSVs

```bash
python pipeline/ecommerce_clean.py
```

Outputs:

- `data/ecommerce_cleaned.csv`
- `data/ecommerce_sample.csv` (first 100K rows)

### Run the simulator

```bash
# Run all algorithms on sample
py src/cli/main.py --dataset data/ecommerce_sample.csv --algorithm all

# Quick test (first 5K requests)
py src/cli/main.py --dataset data/ecommerce_sample.csv --algorithm all --sample 5000

# Human traffic only
py src/cli/main.py --dataset data/ecommerce_sample.csv --algorithm all --exclude-bots

# Default Command
py src/cli/main.py --algorithm all --sample <any number of requests>


```

Results are written to `results/` (JSON + TXT + PNG charts if matplotlib is installed).

### Tests

```bash
pytest tests/ -v
```

