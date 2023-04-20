# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: discovery.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0f\x64iscovery.proto\"z\n\x0eRegistrantInfo\x12\n\n\x02id\x18\x01 \x01(\t\x12\x11\n\x04\x61\x64\x64r\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x11\n\x04port\x18\x03 \x01(\rH\x01\x88\x01\x01\x12\x16\n\ttimestamp\x18\x04 \x01(\x01H\x02\x88\x01\x01\x42\x07\n\x05_addrB\x07\n\x05_portB\x0c\n\n_timestamp\"T\n\x0bRegisterReq\x12\x13\n\x04role\x18\x01 \x01(\x0e\x32\x05.Role\x12\x1d\n\x04info\x18\x02 \x01(\x0b\x32\x0f.RegistrantInfo\x12\x11\n\ttopiclist\x18\x03 \x03(\t\"C\n\rDeregisterReq\x12\x13\n\x04role\x18\x01 \x01(\x0e\x32\x05.Role\x12\x1d\n\x04info\x18\x02 \x01(\x0b\x32\x0f.RegistrantInfo\"G\n\x0cRegisterResp\x12\x17\n\x06status\x18\x01 \x01(\x0e\x32\x07.Status\x12\x13\n\x06reason\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\t\n\x07_reason\"\x0c\n\nIsReadyReq\"\x1d\n\x0bIsReadyResp\x12\x0e\n\x06status\x18\x01 \x01(\x08\"=\n\x13LookupPubByTopicReq\x12\x13\n\x04role\x18\x01 \x01(\x0e\x32\x05.Role\x12\x11\n\ttopiclist\x18\x02 \x03(\t\";\n\x14LookupPubByTopicResp\x12#\n\npublishers\x18\x01 \x03(\x0b\x32\x0f.RegistrantInfo\"\xd6\x01\n\x0c\x44iscoveryReq\x12\x1b\n\x08msg_type\x18\x01 \x01(\x0e\x32\t.MsgTypes\x12$\n\x0cregister_req\x18\x02 \x01(\x0b\x32\x0c.RegisterReqH\x00\x12\"\n\x0bisready_req\x18\x03 \x01(\x0b\x32\x0b.IsReadyReqH\x00\x12*\n\nlookup_req\x18\x04 \x01(\x0b\x32\x14.LookupPubByTopicReqH\x00\x12(\n\x0e\x64\x65register_req\x18\x05 \x01(\x0b\x32\x0e.DeregisterReqH\x00\x42\t\n\x07\x43ontent\"\xb3\x01\n\rDiscoveryResp\x12\x1b\n\x08msg_type\x18\x01 \x01(\x0e\x32\t.MsgTypes\x12&\n\rregister_resp\x18\x02 \x01(\x0b\x32\r.RegisterRespH\x00\x12$\n\x0cisready_resp\x18\x03 \x01(\x0b\x32\x0c.IsReadyRespH\x00\x12,\n\x0blookup_resp\x18\x04 \x01(\x0b\x32\x15.LookupPubByTopicRespH\x00\x42\t\n\x07\x43ontent*P\n\x04Role\x12\x10\n\x0cROLE_UNKNOWN\x10\x00\x12\x12\n\x0eROLE_PUBLISHER\x10\x01\x12\x13\n\x0fROLE_SUBSCRIBER\x10\x02\x12\r\n\tROLE_BOTH\x10\x03*\\\n\x06Status\x12\x12\n\x0eSTATUS_UNKNOWN\x10\x00\x12\x12\n\x0eSTATUS_SUCCESS\x10\x01\x12\x12\n\x0eSTATUS_FAILURE\x10\x02\x12\x16\n\x12STATUS_CHECK_AGAIN\x10\x03*\x8e\x01\n\x08MsgTypes\x12\x10\n\x0cTYPE_UNKNOWN\x10\x00\x12\x11\n\rTYPE_REGISTER\x10\x01\x12\x10\n\x0cTYPE_ISREADY\x10\x02\x12\x1c\n\x18TYPE_LOOKUP_PUB_BY_TOPIC\x10\x03\x12\x18\n\x14TYPE_LOOKUP_ALL_PUBS\x10\x04\x12\x13\n\x0fTYPE_DEREGISTER\x10\x05\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'discovery_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _globals['_ROLE']._serialized_start=939
  _globals['_ROLE']._serialized_end=1019
  _globals['_STATUS']._serialized_start=1021
  _globals['_STATUS']._serialized_end=1113
  _globals['_MSGTYPES']._serialized_start=1116
  _globals['_MSGTYPES']._serialized_end=1258
  _globals['_REGISTRANTINFO']._serialized_start=19
  _globals['_REGISTRANTINFO']._serialized_end=141
  _globals['_REGISTERREQ']._serialized_start=143
  _globals['_REGISTERREQ']._serialized_end=227
  _globals['_DEREGISTERREQ']._serialized_start=229
  _globals['_DEREGISTERREQ']._serialized_end=296
  _globals['_REGISTERRESP']._serialized_start=298
  _globals['_REGISTERRESP']._serialized_end=369
  _globals['_ISREADYREQ']._serialized_start=371
  _globals['_ISREADYREQ']._serialized_end=383
  _globals['_ISREADYRESP']._serialized_start=385
  _globals['_ISREADYRESP']._serialized_end=414
  _globals['_LOOKUPPUBBYTOPICREQ']._serialized_start=416
  _globals['_LOOKUPPUBBYTOPICREQ']._serialized_end=477
  _globals['_LOOKUPPUBBYTOPICRESP']._serialized_start=479
  _globals['_LOOKUPPUBBYTOPICRESP']._serialized_end=538
  _globals['_DISCOVERYREQ']._serialized_start=541
  _globals['_DISCOVERYREQ']._serialized_end=755
  _globals['_DISCOVERYRESP']._serialized_start=758
  _globals['_DISCOVERYRESP']._serialized_end=937
# @@protoc_insertion_point(module_scope)