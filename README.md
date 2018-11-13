# Development server

```
mkdir -p venv
python3 -m venv venv --system-site-packages
. venv/bin/activate
pip install Flask
```

```
. venv/bin/activate
export FLASK_APP=transcoding_cluster_server
export FLASK_ENV=development
export TRANSCODING_CLUSTER_CONFIG=test.ini
flask run
```
