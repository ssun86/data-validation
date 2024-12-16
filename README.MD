## Run Backend API Locally

Environment MongoDB Cloud Console: https://cloud.mongodb.com/v2/664ffb9cc24a4f4ec4a9d75f#/overview


.env.local
```shell
MONGODB_URL=mongodb+srv://{user}:{password}@{MongoDB_url}/?retryWrites=true&w=majority&appName=dev-cms-searchengine
MYSQL_HOST=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_DB=
```

```shell
# create a python environment
python -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt

# run API locally
source venv/bin/activate
source .env.local
uvicorn server:app --reload \
                  --reload-dir ./app
                  --host localhost
                  --port 8080

## Examples
```shell
curl -X 'POST' \
  'http://localhost:8080/api/v1/seriesId' \
  -H 'accept: application/json' \
  -d ''
```
```shell
curl -X 'POST' \
  'http://localhost:8080/api/v1/product' \
  -H 'accept: application/json' \
  -d ''
```


