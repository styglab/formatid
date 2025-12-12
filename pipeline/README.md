# development
## env
```
cd pipeline
uv init --python 3.11
uv add -r requirements.txt
```
## run
```
cd pipeline
# 공고 수집
python -m app.main collect
# 수집된 공고의 첨부파일 다운로드
python -m app.main download
```

