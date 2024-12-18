# Route template

This template shows how to simply create a new route.

## Structure

Simply create a new folder following the `template` folder:

```bash
.
├── app.py
├── apps
│   ├── __init__.py
│   ├── routes
│   │   └── template
│   │       ├── api.py
│   │       ├── __init__.py
│   │       ├── README.md
│   │       └── utils.py
│   └── utils
│       └── utils.py
├── bin
├── config.yml
├── LICENSE
├── README.md
└── requirements.txt
```

It should contain `api.py`, `__init__.py`, and eventually `utils.py`.

## Template

Once, you have implemented the functionalities, launch the application:

```bash
python app.py
```

and check available arguments (`GET` method):

```bash
curl http://localhost:32000/api/v1/template
{
  "args": [
    {
      "description": "explain me",
      "name": "arg1",
      "required": true
    },
    {
      "description": "Output format among json[default], csv, parquet, votable",
      "name": "output-format",
      "required": false
    }
  ]
}
```

Check a valid `POST`:

```bash
curl http://localhost:32000/api/v1/template?arg1=1
[{"1":1},{"1":2},{"1":3}]
```

Test that a missing argument is caught:

```bash
curl http://localhost:32000/api/v1/template?arg3=1
{'status': 'error', 'text': 'A value for `arg1` is required. Use GET to check arguments.\n'}
```
