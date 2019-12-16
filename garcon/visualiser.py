"""
==========
Javascript
==========

Example of writing JSON format graph data and using the D3 Javascript library
to produce an HTML/Javascript drawing.
"""
import json
import argparse
import imp
import flask
import networkx as nx
from networkx.readwrite import json_graph
from garcon import activity
import os


def print_history(function):
    def wrapper(*args, **kwargs):
        func = function(*args, **kwargs)
        print(*args)
        print(func)
        return func
    return wrapper


def get_path():
    script_dir = os.path.dirname(__file__)
    rel_path = "graph/graph.json"
    return os.path.join(script_dir, rel_path)


def get_json_graph(activities):
    G = nx.DiGraph()

    for activity in activities:
        G.add_node(
            1,
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


def get_dependencies(source_flow):

    dependencies = {}

    def get_dependency(name, activity, requires=[]):
        dependencies[activity.name] = [a.name for a in requires]

    source_flow.decider(get_dependency, "context")

    return dependencies


def run_server(activities):
    # write json
    json.dump(get_json_graph(activities), open(get_path(), "w"))
    print("Wrote node-link JSON data to graph/graph.json")

    # Serve the file over http to allow for cross origin requests
    app = flask.Flask(__name__, static_folder="graph")

    @app.route("/")
    def static_proxy():
        return app.send_static_file("graph.html")

    print("\nGo to http://localhost:8000 to see the example\n")
    app.run(port=8000)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Visualise some flows')
    parser.add_argument(
        'flow_name',
        type=str,
        help='python flow file location')
    parser.add_argument(
        'namespace',
        type=str,
        help='python flow file location')
    parser.add_argument(
        'version',
        type=str,
        help='python flow file location')

    args = parser.parse_args()

    flow = imp.load_source("flow", args.flow_name)

    source_flow = flow.Flow(
        args.namespace,
        args.version)

    activities = activity.find_workflow_activities(
        source_flow
    )

    dependencies = get_dependencies(source_flow)

    print(dependencies)

    for a in activities:
        print(a.__dict__)

    # run_server(activities)
