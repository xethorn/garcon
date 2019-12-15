"""
==========
Javascript
==========

Example of writing JSON format graph data and using the D3 Javascript library
to produce an HTML/Javascript drawing.
"""
import json

import flask
import networkx as nx
from networkx.readwrite import json_graph
import os

def print_history(function):
    def wrapper(a1, a2):
        func = function(a1, a2)
        print(func)
        return func
    return wrapper

def get_path():
    script_dir = os.path.dirname(__file__)
    rel_path = "graph/graph.json"
    return os.path.join(script_dir, rel_path)


def get_json_graph():
    G = nx.DiGraph()

    for x in range(1, 6):
        G.add_node(
            x,
            name="Activity {}".format(x),
            desc="This is a cool {}".format(x))

    G.add_edges_from([
        (1, 2),
        (2, 3),
        (2, 4),
        (3, 5),
        (4, 5)])
    # this d3 example uses the name attribute for the mouse-hover value,
    # so add a name to each node

    # write json formatted data
    return json_graph.node_link_data(G)  # node-link format to serialize


def run_server():
    # write json
    json.dump(get_json_graph(), open(get_path(), "w"))
    print("Wrote node-link JSON data to graph/graph.json")

    # Serve the file over http to allow for cross origin requests
    app = flask.Flask(__name__, static_folder="graph")


    @app.route("/")
    def static_proxy():
        return app.send_static_file("graph.html")


    print("\nGo to http://localhost:8000 to see the example\n")
    app.run(port=8000)
