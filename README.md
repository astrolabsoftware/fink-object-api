# Fink object API

This repository contains the code source of the Fink REST API used to access object data stored in tables in Apache HBase.

## Requirements and installation

You will need Python installed (>=3.11) with requirements listed in [requirements.txt](requirements.txt). You will also need [fink-cutout-api](https://github.com/astrolabsoftware/fink-cutout-api) fully installed (which implies Hadoop installed on the machine, and Java 11 at least). For the full installation and deployment, refer as to the [procedure](install/README.md).

## Configuration

First you need to configure the parameters in [config.yml](config.yml):

```yml
# Host and port of the application
HOST: localhost
PORT: 32000

# URL of the fink_cutout_api
CUTOUTAPIURL: http://localhost

# HBase configuration
HBASEIP: localhost
ZOOPORT: 2183

# Table schema (schema_{fink_broker}_{fink_science})
SCHEMAVER: schema_3.1_5.21.14

# Maximum number of rows to
# return in one call
NLIMIT: 10000
```

Make sure that the `SCHEMAVER` is the same you use for your tables in HBase.

TODO:
- [ ] Find a way to automatically sync schema with tables.

## Deployment

### Debug

After starting `fink-cutout-api`, you can simply test the API using:

```bash
python app.py
```

### Production

The application is managed by `gunicorn` and `systemd` (see [install](install/README.md)), and you can simply manage it using:

```bash
# start the application
systemctl start fink_object_api

# reload the application if code changed
systemctl restart fink_object_api

# stop the application
systemctl stop fink_object_api
```

TODO:
- [ ] Add nginx management
- [ ] Add bash scripts under `bin/` to manage both nginx and gunicorn

## Tests

All the routes are extensively tested. To trigger a test on a route, simply run:

```bash
python apps/routes/objects/test.py $HOST:$PORT
```

By replacing `HOST` and `$PORT` with their values (could be the main API instance). If the program exits with no error or message, the test has been successful.

TODO:
- [ ] Make tests more verbose, even is successful.

Alternatively, you can launch all tests using:


```bash
./run_tests.sh --url $HOST:$PORT
```

## Profiling a route

To profile a route, simply use:

```bash
./profile_route.sh --route apps/routes/<route>
```

Depending on the route, you will see the details of the timings and a summary similar to:

```python
Wrote profile results to profiling.py.lprof
Inspect results with:
python -m line_profiler -rmt "profiling.py.lprof"
Timer unit: 1e-06 s

Total time: 0.000241599 s
File: /home/peloton/codes/fink-object-api/apps/routes/template/utils.py
Function: my_function at line 19

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    19                                           @profile                                             
    20                                           def my_function(payload):                            
    21         1        241.6    241.6    100.0      return pd.DataFrame({payload["arg1"]: [1, 2, 3]})


  0.00 seconds - /home/peloton/codes/fink-object-api/apps/routes/template/utils.py:19 - my_function
```

## Adding a new route

You find a [template](apps/routes/template) route to start a new route. Just copy this folder, and modify it with your new route. Alternatively, you can see how other routes are structured to get inspiration. Do not forget to add tests in the [test folder](tests/)!
