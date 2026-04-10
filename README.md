## Project Structure
```
formatid/
├── agent/                           # core, tools, schemas
├── services/
│   ├── api/                         # FastAPI (external interface)
│   │   ├── app/
│   │   │   ├── main.py
│   │   │   ├── routers/
│   │   │   ├── schemas/
│   │   │   └── deps/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   │
│   └── worker/                      # 실행 엔진 (task consumer)
│       ├── app/
│       │   ├── worker.py            # main loop
│       │   ├── dispatcher.py        # task → handler mapping
│       │   ├── registry.py          # task registry
│       │   └── executor.py          # 실행 orchestration
│       ├── Dockerfile
│       └── requirements.txt
│
├── domain/                          # 도메인 데이터 모델
│   ├── bid.py
│   ├── attachment.py
│   ├── document.py
│   └── task.py                      # queue payload schema
│
├── tasks/                           # 작업 정의 (pure function)
│   ├── bid/
│   │
│   └── system/
│       ├── health.py
│       └── cleanup.py
│
├── shared/                          # "인프라/공통 로직"
│   ├── task_registry/               # Redis abstraction
│   │   └── client.py
│   │
│   ├── queue/                       # Redis abstraction
│   │   ├── client.py
│   │   ├── producer.py
│   │   ├── consumer.py
│   │   └── schema.py                # queue message 구조
│   │
│   ├── collectors/                  # 데이터 수집 (API / crawling)
│   │   ├── base.py
│   │   ├── g2b/
│   │   │   ├── client.py
│   │   │   ├── bid.py
│   │   │   └── attachment.py
│   │   └── registry.py
│   │
│   ├── storage/                     # 저장 abstraction
│   │   ├── db.py
│   │   ├── s3.py
│   │   └── local.py
│   │
│   ├── attachments/                 # 파일 처리
│   │   ├── downloader.py
│   │   ├── parser.py                # pdf/docx 등
│   │   └── extractor.py             # 텍스트 추출
│   │
│   └── utils/
│       ├── logger.py
│       ├── time.py
│       └── id_generator.py
│
├── configs/                         # 설정 분리 (중요)
│   ├── settings.py
│   ├── logging.yaml
│   └── constants.py
│
├── infra/
│   ├── docker-compose.yml
│   ├── redis/
│   │   └── redis.conf
│   └── env/
│       ├── api.env
│       └── worker.env
│
├── scripts/                         # 운영용 스크립트
│   ├── enqueue_job.py
│   └── backfill.py
│
└── data/                            # 로컬 캐시 or 임시 저장
```

