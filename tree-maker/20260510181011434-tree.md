```markdown
. 📂 load_balancer
├── 📄 README.md
└── 📂 config/
│  ├── 📄 sim_iphash.yaml
│  ├── 📄 sim_lc.yaml
│  ├── 📄 sim_rr.yaml
│  ├── 📄 sim_wrr.yaml
└── 📂 dataset/
│  └── 📂 archive/
│    ├── 📄 ecommerce_report.txt
└── 📂 docs/
│  ├── 📄 TERMINAL_EXECUTION.md
└── 📂 pipeline/
│  ├── 📄 ecommerce_clean.py
├── 📄 pyproject.toml
└── 📂 results/
└── 📂 src/
│  ├── 📄 __init__.py
│  └── 📂 __pycache__/
│  └── 📂 balancer/
│    ├── 📄 __init__.py
│    └── 📂 __pycache__/
│    ├── 📄 interface.py
│    ├── 📄 ip_hash.py
│    ├── 📄 least_connections.py
│    ├── 📄 round_robin.py
│    ├── 📄 weighted_rr.py
│  └── 📂 cli/
│    ├── 📄 __init__.py
│    └── 📂 __pycache__/
│    ├── 📄 live_dashboard.py
│    ├── 📄 main.py
│  └── 📂 ingestion/
│    ├── 📄 __init__.py
│    └── 📂 __pycache__/
│    ├── 📄 loader.py
│    ├── 📄 models.py
│  └── 📂 load_balancer_sim.egg-info/
│  └── 📂 metrics/
│    ├── 📄 __init__.py
│    └── 📂 __pycache__/
│    ├── 📄 collector.py
│    ├── 📄 exporter.py
│    ├── 📄 host_runtime.py
│  └── 📂 nodes/
│    ├── 📄 __init__.py
│    └── 📂 __pycache__/
│    ├── 📄 node.py
│    ├── 📄 pool.py
│  └── 📂 replayer/
│    ├── 📄 __init__.py
│    └── 📂 __pycache__/
│    ├── 📄 replayer.py
└── 📂 tests/
│  └── 📂 integration/
│    └── 📂 __pycache__/
│    ├── 📄 test_simulation.py
│  └── 📂 unit/
│    └── 📂 __pycache__/
│    ├── 📄 test_algorithms.py
│    ├── 📄 test_host_runtime.py
│    ├── 📄 test_metrics.py
│    ├── 📄 test_models.py
└── 📄 ~MVP_Load_Balancer_BUILD_GUIDE_ECommerce.md.saved.bak
```