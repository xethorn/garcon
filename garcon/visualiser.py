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
from time import time
import networkx as nx
from networkx.readwrite import json_graph
from garcon import activity, event
import boto.swf.layer1 as swf


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

    if(isinstance(activities, dict)):
        for a in activities:
            G.add_node(
                a,
                **activities[a])
    else:
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
        activity.result = {}
        return activity

    source_flow.decider(get_dependency)

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
    path = get_path()
    json.dump(get_json_graph(activities, dependencies), open(path, "w"))
    print("Wrote node-link JSON data to graph/graph.json")

    # Serve the file over http to allow for cross origin requests
    app = flask.Flask(__name__, static_folder="graph")

    @app.route("/")
    def static_proxy():
        return app.send_static_file("graph.html")

    print("\nGo to http://localhost:8000 to see the example\n")
    app.run(port=8000)


def get_closed_executions(flow, domain):

    layer = swf.Layer1()
    executions = layer.list_closed_workflow_executions(
        domain,
        workflow_name=flow.name,
        close_latest_date=time(),
        close_oldest_date=0
        )

    return [closed["execution"] for closed in executions["executionInfos"]]


def get_execution_summary(params, domain):

    layer = swf.Layer1()
    events = layer.get_workflow_execution_history(
        domain,
        params["runId"],
        params["workflowId"])

    return event.make_activity_summary(events["events"])


def aggregate_execution_stats(flow, domain, ref_activities):

    execution_params = get_closed_executions(flow, domain)

    summary_stats = {}

    execution_count = 0

    for params in execution_params:
        summary = get_execution_summary(params, domain)
        for key in summary:
            if key not in summary_stats:
                summary_stats[key] = summary[key]
            else:
                for var in summary[key]:
                    if var not in summary_stats[key]:
                        summary_stats[key][var] = summary[key][var]
                    summary_stats[key][var] += summary[key][var]
        execution_count += 1
        print("processed {} executions".format(execution_count))

    for ref_activity in ref_activities:

        activity_name = ref_activity["name"]

        if activity_name in summary_stats:

            oldstats = summary_stats[activity_name]

            total_duration = 0
            total_runs = 0
            success_n = 0
            failure_n = 0

            if 'success_count' in oldstats:
                total_duration += oldstats['total_time_success']
                total_runs += oldstats['success_count']
                success_n += oldstats['success_count']

            if 'failed_count' in oldstats:
                total_duration += oldstats['total_time_fail']
                total_runs += oldstats['failed_count']
                failure_n += oldstats['failed_count']

            avg_duration = round(total_duration/total_runs)

            summary_stats[activity_name] = {
                "name": activity_name,
                "avg_duration": avg_duration,
                "success_n": success_n,
                "failure_n": failure_n
            }
        else:
            summary_stats[activity_name] = ref_activity

    return summary_stats


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Visualise some flows')
    parser.add_argument(
        'flow_name',
        type=str,
        help='python flow file location')
    parser.add_argument(
        'gtype',
        type=str,
        help='pick from overview(default) or summary')
    parser.add_argument(
        'namespace',
        type=str,
        help='python flow namespace')
    parser.add_argument(
        'version',
        type=str,
        help='python flow version')

    args = parser.parse_args()

    flow = imp.load_source("flow", args.flow_name)

    source_flow = flow.Flow()

    activities = activity.find_workflow_activities(
        source_flow
    )

    cleaned = sanitize_activities(activities)

    dependencies = get_dependencies(source_flow)

    if(args.gtype == "summary"):
        aggregate = aggregate_execution_stats(source_flow, args.namespace, cleaned)
        run_server(aggregate, dependencies)
    else:
        run_server(cleaned, dependencies)
