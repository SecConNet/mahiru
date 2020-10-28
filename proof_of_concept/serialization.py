"""(De)Serializes objects of various kinds to JSON."""
import base64
from datetime import datetime
from typing import (
        Any, Callable, cast, Dict, Generic, Mapping, Optional, Tuple, Type,
        TypeVar, Union)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
        Encoding, load_pem_public_key, PublicFormat)
from dateutil import parser as dateparser

from proof_of_concept.asset import Asset, ComputeAsset, DataAsset, Metadata
from proof_of_concept.definitions import (
        JobSubmission, JSON, PartyDescription, Plan, RegisteredObject,
        RegistryUpdate, ReplicaUpdate, SiteDescription)
from proof_of_concept.policy import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfComputeIn,
        ResultOfDataIn, Rule)
from proof_of_concept.workflow import Job, Workflow, WorkflowStep


Serializable = Union[RegisteredObject, ReplicaUpdate, Rule]


_SerializableT = TypeVar('_SerializableT', bound=Serializable)


def serialize_party_description(party_desc: PartyDescription) -> JSON:
    """Serializes a PartyDescription object to JSON."""
    public_key = party_desc.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
            ).decode('ascii')

    return {
            'name': party_desc.name,
            'public_key': public_key}


def serialize_site_description(site_desc: SiteDescription) -> JSON:
    """Serializes a SiteDescription object to JSON."""
    result = dict()     # type: JSON
    result['name'] = site_desc.name
    result['owner_name'] = site_desc.owner_name
    result['admin_name'] = site_desc.admin_name
    result['endpoint'] = site_desc.endpoint
    result['store'] = site_desc.store
    result['runner'] = site_desc.runner
    result['namespace'] = site_desc.namespace
    return result


def serialize_replica_update(update: ReplicaUpdate[_SerializableT]) -> JSON:
    """Serialize a replica update to JSON."""
    result = dict()     # type: JSON
    result['from_version'] = update.from_version
    result['to_version'] = update.to_version
    result['valid_until'] = update.valid_until.isoformat()
    result['created'] = [serialize(o) for o in update.created]
    result['deleted'] = [serialize(o) for o in update.deleted]
    return result


def serialize_in_asset_collection(rule: InAssetCollection) -> JSON:
    """Serializes an InAssetCollection object to JSON."""
    return {
            'type': 'InAssetCollection',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'asset': rule.asset,
            'collection': rule.collection}


def serialize_in_party_collection(rule: InPartyCollection) -> JSON:
    """Serializes an InPartyCollection object to JSON."""
    return {
            'type': 'InPartyCollection',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'party': rule.party,
            'collection': rule.collection}


def serialize_may_access(rule: MayAccess) -> JSON:
    """Serializes a MayAccess object to JSON."""
    return {
            'type': 'MayAccess',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'party': rule.party,
            'asset': rule.asset}


def serialize_result_of_data_in(rule: ResultOfDataIn) -> JSON:
    """Serializes a ResultOfDataIn object to JSON."""
    return {
            'type': 'ResultOfDataIn',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'data_asset': rule.data_asset,
            'compute_asset': rule.compute_asset,
            'collection': rule.collection}


def serialize_result_of_compute_in(rule: ResultOfComputeIn) -> JSON:
    """Serializes a ResultOfComputeIn object to JSON."""
    return {
            'type': 'ResultOfComputeIn',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'data_asset': rule.data_asset,
            'compute_asset': rule.compute_asset,
            'collection': rule.collection}


def serialize_workflow_step(step: WorkflowStep) -> JSON:
    """Serialize a workflow step to JSON."""
    return {
            'name': step.name,
            'inputs': step.inputs,
            'outputs': step.outputs,
            'compute_asset_id': step.compute_asset_id}


def serialize_workflow(workflow: Workflow) -> JSON:
    """Serialize a workflow to JSON."""
    return {
            'inputs': workflow.inputs,
            'outputs': workflow.outputs,
            'steps': [
                serialize_workflow_step(s) for s in workflow.steps.values()]}


def serialize_job(job: Job) -> JSON:
    """Serialize a plan to JSON."""
    return {
           'workflow': serialize_workflow(job.workflow),
           'inputs': job.inputs}


def serialize_plan(plan: Plan) -> JSON:
    """Serialize a plan to JSON."""
    step_sites = {
            step.name: site_id for step, site_id in plan.step_sites.items()}
    return {
            'input_sites': plan.input_sites,
            'step_sites': step_sites}


def serialize_job_submission(submission: JobSubmission) -> JSON:
    """Serialize a job submission to JSON."""
    return {
            'job': serialize_job(submission.job),
            'plan': serialize_plan(submission.plan)}


def serialize_metadata(metadata: Metadata) -> JSON:
    """Serialize asset metadata to JSON."""
    return {
            'job': serialize_job(metadata.job),
            'item': metadata.item}


def serialize_asset(asset: Asset) -> JSON:
    """Serialize asset to JSON."""
    return {
            'id': asset.id,
            'data': asset.data,
            'metadata': serialize_metadata(asset.metadata)}


_serializers = dict()   # type: Dict[Type, Callable[[Any], JSON]]
_serializers = {
        PartyDescription: serialize_party_description,
        SiteDescription: serialize_site_description,
        ReplicaUpdate: serialize_replica_update,
        InAssetCollection: serialize_in_asset_collection,
        InPartyCollection: serialize_in_party_collection,
        MayAccess: serialize_may_access,
        ResultOfDataIn: serialize_result_of_data_in,
        ResultOfComputeIn: serialize_result_of_compute_in
        }


def serialize(obj: Serializable) -> JSON:
    """Serializes objects to JSON dicts."""
    return _serializers[type(obj)](obj)


def deserialize_party_description(user_input: JSON) -> PartyDescription:
    """Deserializes a PartyDescription."""
    name = user_input['name']
    public_key = load_pem_public_key(
            user_input['public_key'].encode('ascii'), default_backend())
    return PartyDescription(name, public_key)


def deserialize_site_description(user_input: JSON) -> SiteDescription:
    """Deserializes a SiteDescription."""
    return SiteDescription(
            user_input['name'],
            user_input['owner_name'],
            user_input['admin_name'],
            user_input['endpoint'],
            user_input['runner'],
            user_input['store'],
            user_input['namespace'])


def deserialize_registered_object(user_input: JSON) -> RegisteredObject:
    """Deserialize a RegisteredObject."""
    if 'public_key' in user_input:
        return deserialize_party_description(user_input)
    return deserialize_site_description(user_input)


def deserialize_rule(user_input: JSON) -> Rule:
    """Deserialize a Rule."""
    rule = None     # type: Optional[Rule]
    if user_input['type'] == 'InAssetCollection':
        rule = InAssetCollection(user_input['asset'], user_input['collection'])
    elif user_input['type'] == 'InPartyCollection':
        rule = InPartyCollection(user_input['party'], user_input['collection'])
    elif user_input['type'] == 'MayAccess':
        rule = MayAccess(user_input['party'], user_input['asset'])
    elif user_input['type'] == 'ResultOfDataIn':
        rule = ResultOfDataIn(
                user_input['data_asset'], user_input['compute_asset'],
                user_input['collection'])
    elif user_input['type'] == 'ResultOfComputeIn':
        rule = ResultOfComputeIn(
                user_input['data_asset'], user_input['compute_asset'],
                user_input['collection'])
    else:
        raise RuntimeError('Invalid rule type when deserialising')

    rule.signature = base64.urlsafe_b64decode(user_input['signature'])
    return rule


def deserialize_workflow_step(user_input: JSON) -> WorkflowStep:
    """Deserialize a WorkflowStep."""
    return WorkflowStep(
            user_input['name'], user_input['inputs'],
            user_input['outputs'], user_input['compute_asset_id'])


def deserialize_workflow(user_input: JSON) -> Workflow:
    """Deserialize a Workflow."""
    steps = [deserialize_workflow_step(s) for s in user_input['steps']]
    return Workflow(user_input['inputs'], user_input['outputs'], steps)


def deserialize_job(user_input: JSON) -> Job:
    """Deserialize a Job."""
    workflow = deserialize_workflow(user_input['workflow'])
    return Job(workflow, user_input['inputs'])


def deserialize_plan(user_input: JSON, workflow: Workflow) -> Plan:
    """Deserialize a Plan.

    Be sure to validate first if the input is untrusted.

    Args:
        user_input: Trusted user input in JSON.
        workflow: The workflow this plan is for and should refer to.

    Returns:
        The corresponding Plan object.
    """
    step_sites = {
            workflow.steps[step_name]: site_id
            for step_name, site_id in user_input['step_sites'].items()}
    return Plan(user_input['input_sites'], step_sites)


def deserialize_job_submission(user_input: JSON) -> Tuple[Job, Plan]:
    """Deserialize a JobSubmission.

    Be sure to validate first if the input is untrusted. Note that
    this is slightly different, because there is no corresponding
    Python class, instead we deserialize to a tuple.

    Args:
        user_input: Trusted user input in JSON.

    Returns:
        A tuple (job, plan).
    """
    job = deserialize_job(user_input['job'])
    plan = deserialize_plan(user_input['plan'], job.workflow)
    return job, plan


def deserialize_metadata(user_input: JSON) -> Metadata:
    """Deserialize Metadata."""
    job = deserialize_job(user_input['job'])
    return Metadata(job, user_input['item'])


def deserialize_asset(user_input: JSON) -> Asset:
    """Deserialize an Asset."""
    if user_input['data'] is None:
        return ComputeAsset(
                user_input['id'], user_input['data'], user_input['metadata'])
    return DataAsset(
            user_input['id'], user_input['data'], user_input['metadata'])


_deserialize = {
        'Site': deserialize_site_description,
        'Party': deserialize_party_description,
        'RegisteredObject': deserialize_registered_object,
        'Rule': deserialize_rule
        }    # type: Dict[str, Callable[[JSON], Any]]


T = TypeVar('T')


def deserialize_replica_update(
        content_type_tag: str,
        user_input: JSON
        ) -> ReplicaUpdate[T]:
    """Deserialize a ReplicaUpdate.

    Be sure to validate first if the input is untrusted.

    Args:
        content_type_tag: Name of the type to be deserialized.
        user_input: Untrusted user input, JSON objects.

    Returns:
        The deserialized ReplicaUpdate object.
    """
    return ReplicaUpdate[T](
            user_input['from_version'],
            user_input['to_version'],
            dateparser.isoparse(user_input['valid_until']),
            {_deserialize[content_type_tag](o)
                for o in user_input['created']},
            {_deserialize[content_type_tag](o)
                for o in user_input['deleted']})
