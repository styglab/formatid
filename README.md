# Formatid
```
slogan: Chaos in. Structured out.
message: Format chaotic data with AI
value: Structured, Normalized, Parsable
```

## Overview
## Demo / Screenshots
## Features

## Architecture Overview
## Project Structure
```
formatid/
├── api/
│   ├── app/
│   │   ├── main.py
│   │   ├── routers/
│   │   ├── core/
│   │   ├── schemas/
│   │   ├── models/
│   │   ├── db/
│   │   ├── services/
│   │   └── utils/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
│
├── pipeline/
│   ├── app/
│   │   ├── main.py
│   │   ├── jobs/
│   │   ├── core/                   # DB 연결, 설정(config), 로깅(logging), 공통 클라이언트, 실행필수요소 등
│   │   ├── db/
│   │   ├── services/
│   │   └── utils/                  # 비즈니스 무관 도구 모음: 문자열 처리, 날짜 파싱, 포맷 변환, 헬퍼함수 등
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
│
├── data/
│   ├── 00_raw/
│   ├── 01_filtered/
│   ├── 02_extracted/
│   ├── 03_processed/
│   ├── 04_std/
│   └── 05_embeddings/
│
├── infra/
│   ├── docker-compose.yml
│   ├── api-deployment.yaml
│   ├── pipeline-deployment.yaml
│   ├── redis-deployment.yaml
│   └── README.md
│
├── sandbox/
│
└── README.md
```

## Development Process
### 1. Planning & Requirements
```
파급력이 큰 물품을 우선으로 진행
- CCTV/영상보안 장비
- 네트워크 장비(스위치/라우터/AP) 
- 전선/케이블류 
```
### 2. Data Collection & Preprocessing
```
0. 공고 목록 DB 저장
1. (raw) 나라장터 사이트에서 파일 다운로드 로직 개발 
2. (filtered) 다운로드한 파일 중 규격 관련 파일 추출 
3. (extracted) 텍스트 추출 text
4. (processed) 구성물품 및 규격 jsonl
5. (std) 구성물품 및 규격 표준화
```
### 3. Model Development (LLM / Embedding / Inference)
### 4. Backend Development (API / Services)
### 5. Frontend Development (if applicable)
### 6. Integration & Orchestration
### 7. Testing (Unit / Integration / E2E)
### 8. Deployment (Docker / CI/CD)
### 9. Monitoring & Maintenance
### 10. Future Improvements

## Installation
## Quick Start

## API Reference
## Configuration

## Development Guide
### Coding Style & Conventions
### Linting & Formatting
### How to Run Tests
### Branch & Commit Rules

## Roadmap
## Contributing
## License
## Contact
