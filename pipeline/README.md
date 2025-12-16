# dev
## env
```
# set env
cd pipeline
uv init --python 3.11
uv add -r requirements.txt
```
# run
```
cd pipeline
# run env
source .venv/bin/activate
# 공고 수집
APP_ENV=dev python -m app.main collect --from 202001010000 --to 202001312359 --div 1 --rows 100
# 수집된 공고의 첨부파일 다운로드
APP_ENV=dev python -m app.main download
```

