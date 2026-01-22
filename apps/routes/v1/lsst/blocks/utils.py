# Copyright 2026 AstroLab Software
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
import fink_filters.rubin.blocks as fblocks
import importlib


def extract_blocks(with_description=False):
    """Extract user-defined blocks

    Parameters
    ----------
    with_description: bool
        If True, returns block names and descriptions.
        Otherwise, returns only block names.

    Returns
    -------
    block_names: list of str
        List of block names
    descriptions: list of str, optional
        Long descriptions for blocks
    """
    # User-defined blocks
    block_names = [prop for prop in dir(fblocks) if prop.startswith("b_")]

    if with_description:
        module_name = importlib.import_module(fblocks.__name__)
        descriptions = [
            getattr(module_name, block).__doc__.split("\n")[0] for block in block_names
        ]
        return block_names, descriptions

    return block_names
