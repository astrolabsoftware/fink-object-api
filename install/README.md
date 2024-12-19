# API installation and deployment

Fire a Virtual Machine, and follow instructions. Work perfectly on recent AlmaLinux.

## Python dependencies

Clone this repository, and install all python dependencies:

```bash
pip install -r requirements.txt
```

Run an update of rocks:

```bash
rocks update
```

## Fink cutout API installation

Follow instructions in the [fink-cutout-api](https://github.com/astrolabsoftware/fink-cutout-api/blob/main/install/README.md).

## Client installation and Fink gateway

To access HBase tables, we use a client based on [Lomikel](https://github.com/hrivnac/Lomikel). To download the latest version of the client, go to `bin` and execute:

```bash
cd bin
./download_client.sh
```

Do not forget to update the version in the `config.yml` file. Then install (as sudo) a new unit for systemd under `/etc/systemd/system/fink_gateway.service` (check the correct version numbers for JARs):

```bash
[Unit]
Description=Start a JVM with Fink Java objects
After=network.target

[Service]
User=root
Group=root
WorkingDirectory=/opt/fink-object-api/bin

ExecStart=/bin/sh -c 'source /root/.bashrc; exec java -cp "Lomikel-03.04.00x-HBase.exe.jar:py4j0.10.9.7.jar:gson-2.11.0.jar" com.Lomikel.Py4J.LomikelGatewayServer 2>&1 >> /tmp/fink_gateway.out'

[Install]
WantedBy=multi-user.target
```

Reload daemon and start the service:

```bash
systemctl daemon-reload
systemctl enable fink_gateway
systemctl start fink_gateway
```

Check carefuly the status:

```bash
systemctl status fink_gateway
```

Note that having a JVM open all the time can lead to a memory leak, so it is probably wise to restart the service from time to time.


## Systemctl and gunicorn

Install a new unit (as sudo) for systemd under `/etc/systemd/system/fink_object_api.service`:

```bash
[Unit]
Description=gunicorn daemon for fink_object_api
After=network.target fink_gateway.service fink_cutout_api.service
Requires=fink_gateway.service fink_cutout_api.service

[Service]
User=root
Group=root
WorkingDirectory=/opt/fink-object-api

ExecStart=/bin/sh -c 'source /root/.bashrc; exec /opt/fink-env/bin/gunicorn --log-file=/tmp/fink_object_api.log app:app -b :PORT2 --workers=1 --threads=8 --timeout 180 --chdir /opt/fink-object-api --bind unix:/run/fink_object_api.sock 2>&1 >> /tmp/fink_object_api.out'

[Install]
WantedBy=multi-user.target
```

Make sure you change `PORT2` with your actual port. Make sure also to update path to `gunicorn`. Update the `config.yml`, reload units and launch the application:

```bash
systemctl daemon-reload
systemctl enable fink_object_api
systemctl start fink_object_api
```

Note that this will automatically starts `fink_gateway.service` and `fink_cutout_api.service` if they were not started. You are ready to use the API!
