"""(De)Serialization of objects of various kinds to JSON."""
import base64
from typing import (
        Any, Callable, cast, Dict, Optional, Type, TypeVar, Union)

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import (
        Encoding, load_pem_public_key, PublicFormat)
from dateutil import parser as dateparser

from mahiru.definitions.assets import (
        Asset, ComputeAsset, ComputeMetadata, DataAsset, DataMetadata,
        Metadata)
from mahiru.definitions.connections import (
        ConnectionInfo, ConnectionRequest, WireGuardEndpoint,
        WireGuardConnectionInfo, WireGuardConnectionRequest)
from mahiru.definitions.interfaces import IReplicaUpdate
from mahiru.definitions.execution import JobResult
from mahiru.definitions.policy import Rule
from mahiru.definitions.registry import (
        PartyDescription, RegisteredObject, SiteDescription)
from mahiru.definitions.workflows import (
        ExecutionRequest, Job, Plan, Workflow, WorkflowStep)

from mahiru.policy.definitions import PolicyUpdate
from mahiru.policy.rules import (
        InAssetCollection, InAssetCategory, InPartyCategory, MayAccess, MayUse,
        ResultOfComputeIn, ResultOfDataIn)

from mahiru.registry.replication import RegistryUpdate
from mahiru.replication import ReplicaUpdate
from mahiru.rest.definitions import JSON


T = TypeVar('T')


Serializable = Union[
        Asset, ConnectionInfo, ConnectionRequest, ExecutionRequest, Job,
        JobResult, Metadata, Plan, RegisteredObject, IReplicaUpdate, Rule,
        Workflow, WorkflowStep]


_SerializableT = TypeVar('_SerializableT', bound=Serializable)


# Registered objects


def _serialize_party_description(party_desc: PartyDescription) -> JSON:
    """Serialize a PartyDescription object to JSON."""
    public_key = party_desc.public_key.public_bytes(
            encoding=Encoding.PEM,
            format=PublicFormat.SubjectPublicKeyInfo
            ).decode('ascii')

    return {
            'id': party_desc.id,
            'namespace': party_desc.namespace,
            'public_key': public_key}


def _deserialize_party_description(user_input: JSON) -> PartyDescription:
    """Deserialize a PartyDescription object from JSON."""
    id_ = user_input['id']
    namespace = user_input['namespace']
    public_key = load_pem_public_key(
            user_input['public_key'].encode('ascii'), default_backend())
    return PartyDescription(id_, namespace, public_key)


def _serialize_site_description(site_desc: SiteDescription) -> JSON:
    """Serialize a SiteDescription object to JSON."""
    result = dict()     # type: JSON
    result['id'] = site_desc.id
    result['owner_id'] = site_desc.owner_id
    result['admin_id'] = site_desc.admin_id
    result['endpoint'] = site_desc.endpoint
    result['has_store'] = site_desc.has_store
    result['has_runner'] = site_desc.has_runner
    result['has_policies'] = site_desc.has_policies
    return result


def _deserialize_site_description(user_input: JSON) -> SiteDescription:
    """Deserialize a SiteDescription object from JSON."""
    return SiteDescription(
            user_input['id'],
            user_input['owner_id'],
            user_input['admin_id'],
            user_input['endpoint'],
            user_input['has_store'],
            user_input['has_runner'],
            user_input['has_policies'])


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


def _serialize_in_asset_category(rule: InAssetCategory) -> JSON:
    """Serialize an InAssetCategory object to JSON."""
    return {
            'type': 'InAssetCategory',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'asset': rule.asset,
            'category': rule.category}


def _serialize_in_party_category(rule: InPartyCategory) -> JSON:
    """Serialize an InPartyCategory object to JSON."""
    return {
            'type': 'InPartyCategory',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'party': rule.party,
            'category': rule.category}


def _serialize_may_access(rule: MayAccess) -> JSON:
    """Serialize a MayAccess object to JSON."""
    return {
            'type': 'MayAccess',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'site': rule.site,
            'asset': rule.asset}


def _serialize_may_use(rule: MayUse) -> JSON:
    """Serialize a MayUse object to JSON."""
    return {
            'type': 'MayUse',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'party': rule.party,
            'asset': rule.asset,
            'conditions': rule.conditions}


def _serialize_result_of_data_in(rule: ResultOfDataIn) -> JSON:
    """Serialize a ResultOfDataIn object to JSON."""
    return {
            'type': 'ResultOfDataIn',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'data_asset': rule.data_asset,
            'compute_asset': rule.compute_asset,
            'output': rule.output,
            'collection': rule.collection}


def _serialize_result_of_compute_in(rule: ResultOfComputeIn) -> JSON:
    """Serialize a ResultOfComputeIn object to JSON."""
    return {
            'type': 'ResultOfComputeIn',
            'signature': base64.urlsafe_b64encode(rule.signature).decode(),
            'data_asset': rule.data_asset,
            'compute_asset': rule.compute_asset,
            'output': rule.output,
            'collection': rule.collection}


def _deserialize_rule(user_input: JSON) -> Rule:
    """Deserialize a Rule from JSON."""
    rule = None     # type: Optional[Rule]
    if user_input['type'] == 'InAssetCollection':
        rule = InAssetCollection(user_input['asset'], user_input['collection'])
    elif user_input['type'] == 'InAssetCategory':
        rule = InAssetCategory(user_input['asset'], user_input['category'])
    elif user_input['type'] == 'InPartyCategory':
        rule = InPartyCategory(user_input['party'], user_input['category'])
    elif user_input['type'] == 'MayAccess':
        rule = MayAccess(user_input['site'], user_input['asset'])
    elif user_input['type'] == 'MayUse':
        rule = MayUse(
                user_input['party'], user_input['asset'],
                user_input['conditions'])
    elif user_input['type'] == 'ResultOfDataIn':
        rule = ResultOfDataIn(
                user_input['data_asset'], user_input['compute_asset'],
                user_input['output'], user_input['collection'])
    elif user_input['type'] == 'ResultOfComputeIn':
        rule = ResultOfComputeIn(
                user_input['data_asset'], user_input['compute_asset'],
                user_input['output'], user_input['collection'])
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
            'submitter': job.submitter,
            'workflow': _serialize_workflow(job.workflow),
            'inputs': job.inputs}


def _deserialize_job(user_input: JSON) -> Job:
    """Deserialize a Job from JSON."""
    workflow = _deserialize_workflow(user_input['workflow'])
    return Job(user_input['submitter'], workflow, user_input['inputs'])


def _serialize_plan(plan: Plan) -> JSON:
    """Serialize a Plan to JSON."""
    return {'step_sites': plan.step_sites}


def _deserialize_plan(user_input: JSON) -> Plan:
    """Deserialize a Plan from JSON."""
    return Plan(user_input['step_sites'])


def _serialize_execution_request(request: ExecutionRequest) -> JSON:
    """Serialize an execution request to JSON."""
    return {
            'job': _serialize_job(request.job),
            'plan': _serialize_plan(request.plan)}


def _deserialize_execution_request(user_input: JSON) -> ExecutionRequest:
    """Deserialize an ExecutionRequest from JSON."""
    job = _deserialize_job(user_input['job'])
    plan = _deserialize_plan(user_input['plan'])
    return ExecutionRequest(job, plan)


# Assets and metadata


def _serialize_compute_metadata(metadata: ComputeMetadata) -> JSON:
    """Serialize a ComputeMetadata to JSON."""
    return {
            'output_base': metadata.output_base}


def _serialize_data_metadata(metadata: DataMetadata) -> JSON:
    """Serialize a DataMetadata to JSON."""
    return {
            'job': _serialize_job(metadata.job),
            'item': metadata.item}


def _serialize_metadata(metadata: Metadata) -> JSON:
    """Serialize a Metadata to JSON."""
    if isinstance(metadata, ComputeMetadata):
        return _serialize_compute_metadata(metadata)
    if isinstance(metadata, DataMetadata):
        return _serialize_data_metadata(metadata)
    raise RuntimeError('Invalid Metadata type')


def _deserialize_compute_metadata(user_input: JSON) -> ComputeMetadata:
    """Deserialize a ComputeMetadata from JSON."""
    return ComputeMetadata(user_input['output_base'])


def _deserialize_data_metadata(user_input: JSON) -> DataMetadata:
    """Deserialize a DataMetadata from JSON."""
    job = _deserialize_job(user_input['job'])
    return DataMetadata(job, user_input['item'])


def _serialize_asset(asset: Asset) -> JSON:
    """Serialize an Asset to JSON."""
    result = {
            'id': asset.id,
            'kind': asset.kind,
            'data': asset.data,
            'image_location': asset.image_location}

    if asset.metadata is not None:
        result['metadata'] = _serialize_metadata(asset.metadata)

    return result


def _deserialize_asset(user_input: JSON) -> Asset:
    """Deserialize an Asset from JSON."""
    if user_input['kind'] == 'compute':
        return ComputeAsset(
                user_input['id'], user_input['data'],
                user_input['image_location'],
                _deserialize_compute_metadata(user_input['metadata']))
    elif user_input['kind'] == 'data':
        return DataAsset(
                user_input['id'], user_input['data'],
                user_input['image_location'],
                _deserialize_data_metadata(user_input['metadata']))
    else:
        raise RuntimeError('Invalid asset type when deserialising')


def _serialize_compute_asset(asset: ComputeAsset) -> JSON:
    """Serialize a ComputeAsset to JSON."""
    return _serialize_asset(asset)


def _serialize_data_asset(asset: DataAsset) -> JSON:
    """Serialize a DataAsset to JSON."""
    return _serialize_asset(asset)


# Connections

def _serialize_wireguard_endpoint(endpoint: WireGuardEndpoint) -> JSON:
    return {
            'address': endpoint.address,
            'port': endpoint.port,
            'key': endpoint.key}


def _deserialize_wireguard_endpoint(user_input: JSON) -> WireGuardEndpoint:
    return WireGuardEndpoint(
            user_input['address'], user_input['port'], user_input['key'])


def _serialize_wireguard_connection_request(
        request: WireGuardConnectionRequest) -> JSON:
    return {
            'net': request.net,
            'endpoint': _serialize_wireguard_endpoint(request.endpoint)}


def _deserialize_connection_request(user_input: JSON) -> ConnectionRequest:
    return WireGuardConnectionRequest(
            user_input['net'],
            _deserialize_wireguard_endpoint(user_input['endpoint']))


def _serialize_wireguard_connection_info(
        connection: WireGuardConnectionInfo) -> JSON:
    return {
            'conn_id': connection.conn_id,
            'endpoint': _serialize_wireguard_endpoint(connection.endpoint)}


def _deserialize_connection_info(user_input: JSON) -> ConnectionInfo:
    return WireGuardConnectionInfo(
            user_input['conn_id'],
            _deserialize_wireguard_endpoint(user_input['endpoint']))


# Results

def _serialize_job_result(result: JobResult) -> JSON:
    """Serialize a JobResult to JSON."""
    return {
            'job': _serialize_job(result.job),
            'plan': _serialize_plan(result.plan),
            'is_done': result.is_done,
            'outputs': {
                name: _serialize_asset(asset)
                for name, asset in result.outputs.items()}}


def _deserialize_job_result(user_input: JSON) -> JobResult:
    """Deserialize a JobResult from JSON."""
    job = _deserialize_job(user_input['job'])
    plan = _deserialize_plan(user_input['plan'])
    is_done = user_input['is_done']
    outputs = dict()    # type: Dict[str, Asset]
    for name, asset in user_input['outputs'].items():
        outputs[name] = _deserialize_asset(asset)
    return JobResult(job, plan, is_done, outputs)


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
        InAssetCategory: _serialize_in_asset_category,
        InPartyCategory: _serialize_in_party_category,
        MayAccess: _serialize_may_access,
        MayUse: _serialize_may_use,
        ResultOfDataIn: _serialize_result_of_data_in,
        ResultOfComputeIn: _serialize_result_of_compute_in,
        WorkflowStep: _serialize_workflow_step,
        Workflow: _serialize_workflow,
        Job: _serialize_job,
        Plan: _serialize_plan,
        ExecutionRequest: _serialize_execution_request,
        ComputeMetadata: _serialize_compute_metadata,
        DataMetadata: _serialize_data_metadata,
        ComputeAsset: _serialize_compute_asset,
        DataAsset: _serialize_data_asset,
        WireGuardConnectionInfo: _serialize_wireguard_connection_info,
        WireGuardConnectionRequest: _serialize_wireguard_connection_request,
        JobResult: _serialize_job_result,
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
        ExecutionRequest: _deserialize_execution_request,
        ComputeMetadata: _deserialize_compute_metadata,
        DataMetadata: _deserialize_data_metadata,
        Asset: _deserialize_asset,
        ConnectionInfo: _deserialize_connection_info,
        ConnectionRequest: _deserialize_connection_request,
        JobResult: _deserialize_job_result,
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
