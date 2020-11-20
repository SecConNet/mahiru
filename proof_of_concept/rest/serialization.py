"""(De)Serialization of objects of various kinds to JSON."""
import base64
from typing import (
        Any, Callable, cast, Dict, Optional, Type, TypeVar, Union)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
        Encoding, load_pem_public_key, PublicFormat)
from dateutil import parser as dateparser

from proof_of_concept.definitions.assets import (
        Asset, ComputeAsset, DataAsset, Metadata)
from proof_of_concept.definitions.interfaces import IReplicaUpdate
from proof_of_concept.definitions.policy import Rule
from proof_of_concept.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from proof_of_concept.definitions.workflows import (
        Job, JobSubmission, Plan, Workflow, WorkflowStep)

from proof_of_concept.policy.definitions import PolicyUpdate
from proof_of_concept.policy.rules import (
        InAssetCollection, InPartyCollection, MayAccess, ResultOfComputeIn,
        ResultOfDataIn)

from proof_of_concept.registry.replication import RegistryUpdate
from proof_of_concept.replication import ReplicaUpdate
from proof_of_concept.rest.definitions import JSON


T = TypeVar('T')


Serializable = Union[
        Asset, Job, JobSubmission, Metadata, Plan, RegisteredObject,
        IReplicaUpdate, Rule, Workflow, WorkflowStep]


_SerializableT = TypeVar('_SerializableT', bound=Serializable)


# Registered objects


def _serialize_party_description(party_desc: PartyDescription) -> JSON:
    """Serialize a PartyDescription object to JSON."""
    public_key = party_desc.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
            ).decode('ascii')

    return {
            'name': party_desc.name,
            'public_key': public_key}


def _deserialize_party_description(user_input: JSON) -> PartyDescription:
    """Deserialize a PartyDescription object from JSON."""
    name = user_input['name']
    public_key = load_pem_public_key(
            user_input['public_key'].encode('ascii'), default_backend())
    return PartyDescription(name, public_key)


def _serialize_site_description(site_desc: SiteDescription) -> JSON:
    """Serialize a SiteDescription object to JSON."""
    result = dict()     # type: JSON
    result['name'] = site_desc.name
    result['owner_name'] = site_desc.owner_name
    result['admin_name'] = site_desc.admin_name
    result['endpoint'] = site_desc.endpoint
    result['store'] = site_desc.store
    result['runner'] = site_desc.runner
    result['namespace'] = site_desc.namespace
    return result


def _deserialize_site_description(user_input: JSON) -> SiteDescription:
    """Deserialize a SiteDescription object from JSON."""
    return SiteDescription(
            user_input['name'],
            user_input['owner_name'],
            user_input['admin_name'],
            user_input['endpoint'],
            user_input['runner'],
            user_input['store'],
            user_input['namespace'])


def _deserialize_registered_object(user_input: JSON) -> RegisteredObject:
    """Deserialize a RegisteredObject object from JSON."""
    if 'public_key' in user_input:
        return _deserialize_party_description(user_input)
    return _deserialize_site_description(user_input)


# Rules


def _serialize_in_asset_collection(rule: InAssetCollection) -> JSON:
    """Serialize an InAssetCollection object to JSON."""
    return {
            'type': 'InAssetCollection',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'asset': rule.asset,
            'collection': rule.collection}


def _serialize_in_party_collection(rule: InPartyCollection) -> JSON:
    """Serialize an InPartyCollection object to JSON."""
    return {
            'type': 'InPartyCollection',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'party': rule.party,
            'collection': rule.collection}


def _serialize_may_access(rule: MayAccess) -> JSON:
    """Serialize a MayAccess object to JSON."""
    return {
            'type': 'MayAccess',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'site': rule.site,
            'asset': rule.asset}


def _serialize_result_of_data_in(rule: ResultOfDataIn) -> JSON:
    """Serialize a ResultOfDataIn object to JSON."""
    return {
            'type': 'ResultOfDataIn',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'data_asset': rule.data_asset,
            'compute_asset': rule.compute_asset,
            'collection': rule.collection}


def _serialize_result_of_compute_in(rule: ResultOfComputeIn) -> JSON:
    """Serialize a ResultOfComputeIn object to JSON."""
    return {
            'type': 'ResultOfComputeIn',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'data_asset': rule.data_asset,
            'compute_asset': rule.compute_asset,
            'collection': rule.collection}


def _deserialize_rule(user_input: JSON) -> Rule:
    """Deserialize a Rule from JSON."""
    rule = None     # type: Optional[Rule]
    if user_input['type'] == 'InAssetCollection':
        rule = InAssetCollection(
                user_input['asset'], user_input['collection'])
    elif user_input['type'] == 'InPartyCollection':
        rule = InPartyCollection(user_input['party'], user_input['collection'])
    elif user_input['type'] == 'MayAccess':
        rule = MayAccess(user_input['site'], user_input['asset'])
    elif user_input['type'] == 'ResultOfDataIn':
        rule = ResultOfDataIn(
                user_input['data_asset'],
                user_input['compute_asset'],
                user_input['collection'])
    elif user_input['type'] == 'ResultOfComputeIn':
        rule = ResultOfComputeIn(
                user_input['data_asset'],
                user_input['compute_asset'],
                user_input['collection'])
    else:
        raise RuntimeError('Invalid rule type when deserialising')

    rule.signature = base64.urlsafe_b64decode(user_input['signature'])
    return rule


# Workflows and jobs


def _serialize_workflow_step(step: WorkflowStep) -> JSON:
    """Serialize a workflow step to JSON."""
    return {
            'name': step.name,
            'inputs': step.inputs,
            'outputs': step.outputs,
            'compute_asset_id': step.compute_asset_id}


def _deserialize_workflow_step(user_input: JSON) -> WorkflowStep:
    """Deserialize a WorkflowStep from JSON."""
    return WorkflowStep(
            user_input['name'], user_input['inputs'],
            user_input['outputs'], user_input['compute_asset_id'])


def _serialize_workflow(workflow: Workflow) -> JSON:
    """Serialize a workflow to JSON."""
    return {
            'inputs': workflow.inputs,
            'outputs': workflow.outputs,
            'steps': [
                _serialize_workflow_step(s) for s in workflow.steps.values()]}


def _deserialize_workflow(user_input: JSON) -> Workflow:
    """Deserialize a Workflow from JSON."""
    steps = [_deserialize_workflow_step(s) for s in user_input['steps']]
    return Workflow(user_input['inputs'], user_input['outputs'], steps)


def _serialize_job(job: Job) -> JSON:
    """Serialize a Job to JSON."""
    return {
           'workflow': _serialize_workflow(job.workflow),
           'inputs': {
               name: asset_id for name, asset_id in job.inputs.items()}}


def _deserialize_job(user_input: JSON) -> Job:
    """Deserialize a Job from JSON."""
    workflow = _deserialize_workflow(user_input['workflow'])
    return Job(workflow, user_input['inputs'])


def _serialize_plan(plan: Plan) -> JSON:
    """Serialize a Plan to JSON."""
    return {
            'input_sites': plan.input_sites,
            'step_sites': plan.step_sites}


def _deserialize_plan(user_input: JSON) -> Plan:
    """Deserialize a Plan from JSON."""
    return Plan(user_input['input_sites'], user_input['step_sites'])


def _serialize_job_submission(submission: JobSubmission) -> JSON:
    """Serialize a job submission to JSON."""
    return {
            'job': _serialize_job(submission.job),
            'plan': _serialize_plan(submission.plan)}


def _deserialize_job_submission(user_input: JSON) -> JobSubmission:
    """Deserialize a JobSubmission from JSON."""
    job = _deserialize_job(user_input['job'])
    plan = _deserialize_plan(user_input['plan'])
    return JobSubmission(job, plan)


# Assets and metadata


def _serialize_metadata(metadata: Metadata) -> JSON:
    """Serialize a Metadata to JSON."""
    return {
            'job': _serialize_job(metadata.job),
            'item': metadata.item}


def _deserialize_metadata(user_input: JSON) -> Metadata:
    """Deserialize a Metadata from JSON."""
    job = _deserialize_job(user_input['job'])
    return Metadata(job, user_input['item'])


def _serialize_asset(asset: Asset) -> JSON:
    """Serialize an Asset to JSON."""
    return {
            'id': asset.id,
            'data': asset.data,
            'metadata': _serialize_metadata(asset.metadata)}


def _deserialize_asset(user_input: JSON) -> Asset:
    """Deserialize an Asset from JSON."""
    if user_input['data'] is None:
        return ComputeAsset(
                user_input['id'], user_input['data'],
                user_input['metadata'])
    return DataAsset(
            user_input['id'], user_input['data'],
            user_input['metadata'])


def _serialize_compute_asset(asset: ComputeAsset) -> JSON:
    """Serialize a ComputeAsset to JSON."""
    return _serialize_asset(asset)


def _serialize_data_asset(asset: DataAsset) -> JSON:
    """Serialize a DataAsset to JSON."""
    return _serialize_asset(asset)


# Replica updates


def _serialize_replica_update(update: IReplicaUpdate[_SerializableT]) -> JSON:
    """Serialize a ReplicaUpdate to JSON."""
    result = dict()     # type: JSON
    result['from_version'] = update.from_version
    result['to_version'] = update.to_version
    result['valid_until'] = update.valid_until.isoformat()
    result['created'] = [serialize(o) for o in update.created]
    result['deleted'] = [serialize(o) for o in update.deleted]
    return result


AnyReplicaUpdate = TypeVar('AnyReplicaUpdate', bound=ReplicaUpdate)


def _deserialize_replica_update(
        update_type: Type[AnyReplicaUpdate], user_input: JSON
        ) -> AnyReplicaUpdate:
    """Deserialize a ReplicaUpdate from JSON."""
    return update_type(
            user_input['from_version'],
            user_input['to_version'],
            dateparser.isoparse(user_input['valid_until']),
            {deserialize(update_type.ReplicatedType, o)
                for o in user_input['created']},
            {deserialize(update_type.ReplicatedType, o)
                for o in user_input['deleted']})


def _deserialize_policy_update(user_input: JSON) -> PolicyUpdate:
    """Deserialize a PolicyUpdate from JSON."""
    return _deserialize_replica_update(PolicyUpdate, user_input)


def _deserialize_registry_update(user_input: JSON) -> RegistryUpdate:
    """Deserialize a RegistryUpdate from JSON."""
    return _deserialize_replica_update(RegistryUpdate, user_input)


_serializers = dict()   # type: Dict[Type, Callable[[Any], JSON]]
_serializers = {
        PartyDescription: _serialize_party_description,
        SiteDescription: _serialize_site_description,
        InAssetCollection: _serialize_in_asset_collection,
        InPartyCollection: _serialize_in_party_collection,
        MayAccess: _serialize_may_access,
        ResultOfDataIn: _serialize_result_of_data_in,
        ResultOfComputeIn: _serialize_result_of_compute_in,
        WorkflowStep: _serialize_workflow_step,
        Workflow: _serialize_workflow,
        Job: _serialize_job,
        Plan: _serialize_plan,
        JobSubmission: _serialize_job_submission,
        Metadata: _serialize_metadata,
        ComputeAsset: _serialize_compute_asset,
        DataAsset: _serialize_data_asset,
        PolicyUpdate: _serialize_replica_update,
        RegistryUpdate: _serialize_replica_update,
        }


def serialize(obj: Serializable) -> JSON:
    """Serialize object to JSON.

    Args:
        obj: An object to serialize.

    Returns:
        Its JSON representation.
    """
    return _serializers[type(obj)](obj)


_deserialize = {
        PartyDescription: _deserialize_party_description,
        SiteDescription: _deserialize_site_description,
        RegisteredObject: _deserialize_registered_object,
        Rule: _deserialize_rule,
        WorkflowStep: _deserialize_workflow_step,
        Workflow: _deserialize_workflow,
        Job: _deserialize_job,
        Plan: _deserialize_plan,
        JobSubmission: _deserialize_job_submission,
        Metadata: _deserialize_metadata,
        Asset: _deserialize_asset,
        PolicyUpdate: _deserialize_policy_update,
        RegistryUpdate: _deserialize_registry_update,
        }


def deserialize(typ: Type[T], user_input: JSON) -> T:
    """Deserializes an object from user input.

    Args:
        typ: The type of object to deserialize.
        user_input: The user's input as a JSON dictionary.
    """
    return cast(T, _deserialize[typ](user_input))
