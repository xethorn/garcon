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
import os
import networkx as nx
from networkx.readwrite import json_graph
from garcon import activity
import boto.swf.layer2 as swf


def print_history(function):
    def wrapper(*args, **kwargs):
        func = function(*args, **kwargs)
        print(func)
        return func
    return wrapper


def get_path():
    script_dir = os.path.dirname(__file__)
    rel_path = "graph/graph.json"
    return os.path.join(script_dir, rel_path)


def get_json_graph(activities, dependencies):
    G = nx.DiGraph()

    for a in activities:
        G.add_node(
            a["name"],
            **a)

    for d in dependencies:
        for req in dependencies[d]:
            G.add_edge(
                req,
                d
            )

    # this d3 example uses the name attribute for the mouse-hover value,
    # so add a name to each node

    # write json formatted data
    return json_graph.node_link_data(G)  # node-link format to serialize


def get_dependencies(source_flow):

    dependencies = {}

    def get_dependency(name, activity, requires=[]):
        dependencies[activity.name] = [a.name for a in requires if a]
        return activity

    source_flow.decider(get_dependency, "context")

    return dependencies


def sanitize_activity(raw_activity):
    return dict((k, raw_activity.__dict__[k]) for k in (
        "name",
        "version",
        "domain",
        "retry",
        "task_list",
        "pool_size",
        "schedule_to_start_timeout"
    ))


def sanitize_activities(activities):

    return [sanitize_activity(a) for a in activities]


def run_server(activities, dependencies):
    # write json
    json.dump(get_json_graph(activities, dependencies), open(get_path(), "w"))
    print("Wrote node-link JSON data to graph/graph.json")

    # Serve the file over http to allow for cross origin requests
    app = flask.Flask(__name__, static_folder="graph")

    @app.route("/")
    def static_proxy():
        return app.send_static_file("graph.html")

    print("\nGo to http://localhost:8000 to see the example\n")
    app.run(port=8000)

def get_closed_executions(flow, domain):
    flow_name=flow.name
    
    executions = swf.list_closed_workflow_executions(domain, workflow_name=flow.name)
    print(executions)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Visualise some flows')
    parser.add_argument(
        'flow_name',
        type=str,
        help='python flow file location')
    parser.add_argument(
        'gtype',
        type=str,
        help='pick from overview, latest, summary')
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

    cleaned = sanitize_activities(activities)

    dependencies = get_dependencies(source_flow)

    get_closed_executions(source_flow, args.namespace)

    for a in activities:
        print(a.__dict__)

    run_server(cleaned, dependencies)
