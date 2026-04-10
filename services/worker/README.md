worker/                      # 실행 엔진 (task consumer)
├── app/
│   ├── worker.py            # main loop
│   ├── dispatcher.py        # task → handler mapping
│   ├── registry.py          # task registry
│   └── executor.py          # 실행 orchestration
├── Dockerfile
└── requirements.txt

