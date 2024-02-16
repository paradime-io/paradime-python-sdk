# First party modules
import json
from pathlib import Path
from typing import List

from pydantic import parse_obj_as

from paradime import Paradime
from paradime.apis.custom_integration.types import Node, NodeType

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Load node types and nodes from JSON files
node_types = parse_obj_as(List[NodeType], json.loads(Path("node_types.json").read_text()))
nodes = parse_obj_as(List[Node], json.loads(Path("nodes.json").read_text()))

# Create a custom integration or update it if it already exists
my_integration = paradime.custom_integration.upsert(
    name="MyParadimeIntegration",
    node_types=node_types,
)

# Add nodes to the custom integration.
paradime.custom_integration.add_nodes(
    integration_uid=my_integration.uid,
    nodes=nodes,
)
