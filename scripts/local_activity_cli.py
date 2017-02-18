import argparse
import importlib
import json

from garcon import activity


def list_activities(flow):
    """Prints all activities associated with a flow.

    Args:
        flow (module): garcon flow module
    """

    print("Activity names include {}".format(
        activity.find_activity_names(flow)))


def run_activity(flow, context, activity_name):
    """Locally run an activity for a flow with a given context

    Args:
        flow (module): garcon flow module
        context (dict): The flow context
        activity_name (str): name of activity (likely  in the format of
            <flow_name>_<activity_name>)
    """

    workflow_activity = activity.find_activity(flow, activity_name)
    if not workflow_activity:
        print("Activity name '{}' not found".format(activity_name))
        print("Valid activities include {}".format(
            activity.find_activity_names(flow)))
        return

    def blank_heartbeat():
        return

    workflow_activity.heartbeat = blank_heartbeat
    result = workflow_activity.execute_activity(context)
    print("Context Result: {}".format(result))


def garcon_local_activity(*args):
    """Main method for local activity cli.
    """

    parser = argparse.ArgumentParser(description='Garcon local activity util')
    parser.add_argument('flow',
                        help='flow module [make sure it is in class path]')

    subparsers = parser.add_subparsers(help='local garcon command', dest='cmd')

    # subparser for list activities cmd
    subparsers.add_parser('list', help='list activities')

    # subparser for run activity cmd
    parser_run = subparsers.add_parser('run', help='run activity')
    parser_run.add_argument(
        'activity', help='Activity to run (<flow_name>_<activity_name>)')
    parser_run.add_argument(
        '-c', '--context', help='initial context [json string]')
    parser_run.add_argument(
        '-cf', '--context-file', help='initial context [json file]')

    # parse cl args (allows easy unit testing)
    args = parser.parse_args(args) if args else parser.parse_args()

    # import the flow module
    flow = importlib.import_module('{}'.format(args.flow))

    if args.cmd == 'run':
        args.context = args.context or '{}'
        args.context = json.loads(args.context)
        #file wins if both params passed
        if(args.context_file):
            with open(args.context_file) as context_file:
                args.context = json.load(context_file)
        args.activity = args.activity or '{}'
        run_activity(flow, args.context, args.activity)
    elif args.cmd == 'list':
        list_activities(flow)


if __name__ == "__main__":
    garcon_local_activity()
