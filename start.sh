export MONGODB_URL=""
export MYSQL_HOST=""
export MYSQL_USER=""
export MYSQL_PASSWORD=''
export MYSQL_DB=""
uvicorn server:app --reload --reload-dir ./app --host localhost --port 8080