# Formatid
```
slogan: Chaos in. Structured out.
message: Format chaotic data with AI
value: Structured, Normalized.
```

## Overview
## Demo / Screenshots
## Features

## Architecture Overview
## Project Structure
```
data/
 ├── 00_raw/                # 원본 파일
 ├── 01_filtered/           # 규격서 관련 파일 (파일 필터링)
 ├── 02_extracted/          # 텍스트/표 구조 추출된 1차 가공본
 ├── 03_processed/          # 물품 규격 파싱/정규화 된 중간 산출물
 ├── 04_std/                # 카테고리/표준 품목명 통합/유사항목 병합된 최종 구조화본
 └── 05_embeddings/         # 임베딩 생성된 데이터
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
