#!/usr/bin/env python3

from mahiru.definitions.workflows import Job, Workflow, WorkflowStep
from mahiru.rest.internal_client import InternalSiteRestClient


if __name__ == '__main__':
    # create single-step workflow
    workflow = Workflow(
            ['input'], {'result': 'compute.output0'}, [
                WorkflowStep(
                    name='compute',
                    inputs={'input0': 'input'},
                    outputs={'output0':
                        'asset:party1_ns:ctd.data.output_base'
                        ':party1_ns:site1'},
                    compute_asset_id=(
                        'asset:party1_ns:ctd.software.script1'
                        ':party1_ns:site1'))
            ]
    )

    inputs = {'input': 'asset:party2_ns:ctd.data.input:party2_ns:site2'}

    # run workflow
    client = InternalSiteRestClient(
            'site:party1_ns:site1', 'http://site1:1080')
    print('Submitting job...')
    job_id = client.submit_job(Job(workflow, inputs))
    print(f'Submitted, waiting for result at {job_id}')
    result = client.get_job_result(job_id)

    print(f'Job complete:')
    print(f'Job: {result.job}')
    print(f'Plan: {result.plan}')
    print(f'Outputs: {result.outputs}')
