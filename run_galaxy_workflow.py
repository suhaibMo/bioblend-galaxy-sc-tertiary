#!/usr/bin/env python
"""run_galaxy_workflow
"""

import argparse
import logging
import os.path
import time
import yaml
from bioblend.galaxy import GalaxyInstance

def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-C', '--conf',
                            required=True,
                            help='A yaml file describing the galaxy credentials')
    arg_parser.add_argument('-P', '--experimentDir',
                            default='',
                            required=True,
                            help='Path to experiment directory folder')
    arg_parser.add_argument('-I', '--instance',
                            default='embassy',
                            help='Galaxy server instance name')
    arg_parser.add_argument('-H', '--history',
                            default='',
                            required=True,
                            help='Name of the history to create')
    arg_parser.add_argument('-W', '--workflow',
                            default='Scanpy_workflow_test',
                            required=True,
                            help='Workflow to run')
    arg_parser.add_argument('--debug',
                            action='store_true',
                            default=False,
                            help='Print debug information')
    args = arg_parser.parse_args()
    return args


def set_logging_level(debug=False):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.WARNING,
        format=('%(asctime)s; %(levelname)s; %(filename)s; %(funcName)s(): '
                '%(message)s'),
        datefmt='%y-%m-%d %H:%M:%S')


def get_instance(conf, name='__default'):
    with open(os.path.expanduser(conf), mode='r') as fh:
        data = yaml.load(fh)
    assert name in data, 'unknown instance'
    entry = data[name]
    if isinstance(entry, dict):
        return entry
    else:
        return data[entry]


def read_workflow_from_file(filename):
    with open(filename, mode='r') as fh:
        wf = yaml.load(fh)
    return wf

def get_workflow_from_name(gi, workflow_name):
    wf = gi.workflows.get_workflows(name=workflow_name)
    return wf

def validate_workflow_tools(wf, tools):
    tool_ids = set([tl['id'] for tl in tools])
    for tool in wf['steps']:
        assert tool['id'] in tool_ids, "unknown tool: {}".format(tool['id'])
    return True

def get_history_id(history_name, histories_obj):
    for hist_dict in histories_obj:
        if hist_dict['name'] == history_name:
            return(hist_dict['id'])

def upload_datasets_from_folder(gi, experimentDir, history_name, history):
    expAcc = os.path.basename(experimentDir)
    files = os.listdir(experimentDir)
    history_id = history['id']
    datasets = []
    # uploading each file from the experiment directory to a history id
    # and record appended files history
    for file in files:
        logging.info('Uploading',file,'for', expAcc, '...')
        file_type = file.split(".")[-1]
        if file_type == 'mtx':
            file_type = 'auto'
        data = gi.tools.upload_file(path=os.path.join(experimentDir,file), history_id = history_id, file_name = file, file_type = file_type)
        datasets.append(data.items())
    return datasets

def get_workflow_id(wf):
    for wf_dic in wf:
        wf_id = wf_dic['id']
    return wf_id

def get_input_data_id(file, wf):
    file_name = os.path.splitext(file)[0].split('_')[1]
    for id in wf['inputs']:
        if wf['inputs'][id]['label'] == file_name:
            input_id = id
    return input_id

def make_data_map(experimentDir, datasets, show_workflow):
    datamap = {}
    files = os.listdir(experimentDir)
    for file in files:
        for idx, hist_dict in enumerate(datasets):
            if datasets[idx][0][1][0]['name'] == file:
                input_data_id = get_input_data_id(file, show_workflow)
                datamap[input_data_id] = { 'src':'hda', 'id': get_history_id(file, datasets[idx][0][1]) }
    if isinstance(datamap, dict):
        return(datamap)


def get_run_state(gi, results):
    results_hid = gi.histories.show_history(results['history'])
    state = results_hid['state']
    return state


def download_results(gi, results, experimentDir):
    results_hid = gi.histories.show_history(results['history'])
    ok_state_ids = results_hid['state_ids']['ok']
    for state_id in ok_state_ids:
        gi.datasets.download_dataset(state_id, file_path=experimentDir, use_default_filename=True)

def main():
    args = get_args()
    set_logging_level(args.debug)

    print('Prepare environment...')
    # Prepare environment
    ins = get_instance(args.conf, name='embassy')
    gi = GalaxyInstance(ins['url'], key=ins['key'])

    print('Create new history to run workflow ...')
    # Create new history to run workflow
    history = gi.histories.create_history(name=args.history)

    print('Uploading dataset to history ...')
    # upload dataset to history
    datasets = upload_datasets_from_folder(gi, args.experimentDir, args.history, history)

    print(args.workflow,'- Workflow setting ...')
    # get saved workflow defined in the galaxy instance
    workflow = get_workflow_from_name(gi, workflow_name = args.workflow)
    workflow_id = get_workflow_id(wf = workflow)
    show_wf =gi.workflows.show_workflow(workflow_id)

    print('Datamap linking uploaded inputs and workflow inputs ...')
    # create input datamap linking uploaded inputs and workflow inputs
    datamap = make_data_map(args.experimentDir, datasets, show_wf)

    print('Running workflow ...')
    ## run work flow
    results = gi.workflows.run_workflow(workflow_id, datamap, history_name = (args.history + '_results'))

    # wait for a little while and check if the status is ok
    time.sleep(20)

    # get_run_state
    state = get_run_state(gi, results)

   # wait until the jobs are completed
    while state == 'queued':
        state = get_run_state(gi, results)
        if state == 'queued':
            time.sleep(10)
            continue
        elif state == 'ok':
            print("jobs ok")
            break

    # Download results
    print('Downloading results ...')
    download_results(gi, results, experimentDir = args.experimentDir)

if __name__ == '__main__':
    main()

