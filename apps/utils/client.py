# Copyright 2023-2024 AstroLab Software
# Author: Julien Peloton
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utilities to work with the Fink HBase client"""

from py4j.java_gateway import JavaGateway

import numpy as np

from apps.utils.utils import extract_configuration

from line_profiler import profile


@profile
def connect_to_hbase_table(
    tablename: str,
    schema_name=None,
    nlimit=10000,
    setphysicalrepo=False,
):
    """Return a client connected to a HBase table

    Parameters
    ----------
    tablename: str
        The name of the table
    schema_name: str, optional
        Name of the rowkey in the table containing the schema. Default is given by the config file.
    nlimit: int, optional
        Maximum number of objects to return. Default is 10000
    config_path: str, optional
        Path to the config file. Default is None (relative to the apps/ folder)
    """
    config = extract_configuration("config.yml")

    gateway = JavaGateway(auto_convert=True)
    client = gateway.jvm.com.Lomikel.HBaser.HBaseClient(
        config["HBASEIP"], config["ZOOPORT"]
    )

    if schema_name is None:
        schema_name = config["SCHEMAVER"]
    client.connect(tablename, schema_name)
    client.setLimit(config["NLIMIT"])

    return client

@profile
def connect_to_graph():
    """Return a client connected to a graph"""
    config = extract_configuration("config.yml")
    gateway = JavaGateway(auto_convert=True)

    jc = gateway.jvm.com.Lomikel.Januser.JanusClient(config["PROPERTIES"])

    # TODO: add definition of IP/PORT/TABLE/SCHEMA here in new version of client
    gr = gateway.jvm.com.astrolabsoftware.FinkBrowser.Januser.FinkGremlinRecipiesG(jc)
    
    return gr, gateway.jvm.com.astrolabsoftware.FinkBrowser.Januser.Classifiers

@profile
def create_or_update_hbase_table(
    tablename: str,
    families: list,
    schema_name: str,
    schema: dict,
    create=False,
):
    """Create or update a table in HBase

    By default (create=False), it will only update the schema of the table
    otherwise it will create the table in HBase and push the schema. The schema
    has a rowkey `schema`.

    Currently accepts only a single family name

    Parameters
    ----------
    tablename: str
        The name of the table
    families: list
        List of family names, e.g. ['d']
    schema_name: str
        Rowkey value for the schema
    schema: dict
        Dictionary with column names (keys) and column types (values)
    create: bool
        If true, create the table. Default is False (only update schema)
    config_path: str, optional
        Path to the config file. Default is None (relative to the apps/ folder)
    """
    if len(np.unique(families)) != 1:
        raise NotImplementedError("`create_hbase_table` only accepts one family name")

    config = extract_configuration("config.yml")

    gateway = JavaGateway(auto_convert=True)
    client = gateway.jvm.com.Lomikel.HBaser.HBaseClient(
        config["HBASEIP"], config["ZOOPORT"]
    )

    if create:
        # Create the table and connect without schema
        client.create(tablename, families)
        client.connect(tablename)
    else:
        # Connect by ignoring the current schema
        client.connect(tablename, None)

    # Push the schema
    out = [f"{families[0]}:{colname}:{coltype}" for colname, coltype in schema.items()]
    client.put(schema_name, out)

    client.close()
