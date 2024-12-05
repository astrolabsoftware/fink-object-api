# API installation and deployment

Fire a Virtual Machine, and follow instructions. Work perfectly on recent AlmaLinux.

## Python dependencies

Clone this repository, and install all python dependencies:

```bash
pip install -r requirements.txt
```

## Fink cutout API installation

Follow instructions in the [fink-cutout-api](https://github.com/astrolabsoftware/fink-cutout-api/blob/main/install/README.md).

## Systemctl and gunicorn

Install a new unit for systemd under `/etc/systemd/system/fink_object_api.service`:

```bash
[Unit]
Description=gunicorn daemon for fink_object_api
After=network.target

[Service]
User=almalinux
Group=almalinux
WorkingDirectory=/home/almalinux/fink-object-api

ExecStart=/bin/sh -c 'source /home/almalinux/.bashrc; exec /home/almalinux/fink-env/bin/gunicorn --log-file=/tmp/fink_object_api.log app:app -b localhost:PORT2 --workers=1 --threads=8 --timeout 180 --chdir /home/almalinux/fink-object-api --bind unix:/home/almalinux/fink_object_api.sock 2>&1 >> /tmp/fink_object_api.out'

[Install]
WantedBy=multi-user.target
```

Make sure you change `PORT2` with your actual port, and `localhost` with your domain. Make sure also to update path to `gunicorn`. Update the `config.yml`, reload units and launch the application:

```bash
sudo systemctl daemon-reload
sudo systemctl start fink_object_api
```


You are ready to use the API!
