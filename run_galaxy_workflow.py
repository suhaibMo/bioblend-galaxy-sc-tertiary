#!/usr/bin/env python
"""run_galaxy_workflow
"""

import argparse
import logging
import os.path
import re
import time
import yaml
from bioblend.galaxy import GalaxyInstance
from pprint import pprint

def get_args():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-C', '--conf',
                            required=True,
                            help='A yaml file describing the workflow')
    arg_parser.add_argument('-P', '--experimentDir',
                            default='',
                            help='Path to experiment directory folder')
    arg_parser.add_argument('-I', '--instance',
                            default='embassy',
                            help='Galaxy server instance name')
    arg_parser.add_argument('-H', '--history',
                            default='scanpy',
                            help='Name of the history to create')
    arg_parser.add_argument('-V', '--variable',
                            default=[],
                            help='A list of tool parameters to vary')
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


def get_instance(name='__default'):
    with open(os.path.expanduser('~/Workflows/bioblend_galaxy/cloud_credentials.yml'), mode='r') as fh:
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

def get_history_id (history_name, histories_obj):
    for hist_dict in histories_obj:
        if hist_dict['name'] == history_name:
            return(hist_dict['id'])

def upload_datasets_from_folder(experimentDir, history_name, history):
    expAcc = os.path.basename(experimentDir)
    files = os.listdir(experimentDir)
    history_id = get_history_id(history_name, history)
    datasets = []
    # uploading each file from the experiment directory to history id
    # and record appended files history
    for file in files:
        logging.info('Uploading',file,'for', expAcc, '...')
        file_type = file.split(".")[-1]
        if file_type == 'mtx':
            file_type = 'auto'
        data = gi.tools.upload_file(path=os.path.join(experimentDir,file), history_id = history_id, file_name = file, file_type = file_type)
        datasets.append(data.items())
    return datasets

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

def get_workflow_id(workflow):
    for wf_dic in workflow:
        workflow_id = wf_dic['id']
    return workflow_id

conf="/Users/suhaib/Workflows/bioblend_galaxy/bioblend-galaxy-sc-tertiary/test/scanpy_workflow.yml"
history_name='scanpy_test1'
debug=True
experimentDir='/Users/suhaib/Workflows/bioblend_galaxy/E-MTAB-101'


def main():
    args = get_args()
    set_logging_level(args.debug)

    # Prepare environment
    ins = get_instance(name='embassy')
    gi = GalaxyInstance(ins['url'], key=ins['key'])

    # Create new history to run workflow
    history = gi.histories.create_history(name=history_name)

    # TODO if the history already existing retrieve history
    #history = gi.histories.get_histories(name=history_name)
    #pprint(history)

    # upload dataset to history
    datasets = upload_datasets_from_folder(experimentDir, history_name, history)

    # get workflow defined in the galaxy
    workflow = get_workflow_from_name(gi, workflow_name = 'Scanpy_workflow_test')
    workflow_id = get_workflow_id(workflow)
    show_wf =gi.workflows.show_workflow(workflow_id)

    # datamap
    datamap = make_data_map(experimentDir,datasets, show_wf)

    ## run work flow
    results = gi.workflows.run_workflow( workflow_id , datamap, history_name = (history_name + '_results'))

    pprint(results)
    # wait for a little while between each job submission
    time.sleep(20)

    ## TODO download results
    return 0

if __name__ == '__main__':
    main()
