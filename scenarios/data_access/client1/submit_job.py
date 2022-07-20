#!/usr/bin/env python3

from pathlib import Path

from mahiru.definitions.workflows import Job, Workflow, WorkflowStep
from mahiru.rest.internal_client import InternalSiteRestClient


CERTS_DIR = Path.home() / 'mahiru' / 'certs'


if __name__ == '__main__':
    # create single-step workflow
    workflow = Workflow(
            ['input'], {'result': 'compute.output0'}, [
                WorkflowStep(
                    name='compute',
                    inputs={'input0': 'input'},
                    outputs={'output0':
                        'asset:party1.mahiru.example.org:da.data.output_base'
                        ':party1.mahiru.example.org:site1'},
                    compute_asset_id=(
                        'asset:party1.mahiru.example.org:da.software.script1'
                        ':party1.mahiru.example.org:site1'))
            ]
    )

    inputs = {
            'input':
                'asset:party2.mahiru.example.org:da.data.input'
                ':party2.mahiru.example.org:site2'}

    # run workflow
    client = InternalSiteRestClient(
            'party:party1.mahiru.example.org:party1',
            'site:party1.mahiru.example.org:site1',
            'https://site1.mahiru.example.org:1443',
            CERTS_DIR / 'trust_store.pem')
    print('Submitting job...')
    job_id = client.submit_job(Job(
        'party:party1.mahiru.example.org:party1', workflow, inputs))
    print(f'Submitted, waiting for result at {job_id}')
    result = client.get_job_result(job_id)

    print(f'Job complete:')
    print(f'Job: {result.job}')
    print(f'Plan: {result.plan}')
    print(f'Outputs: {result.outputs}')
