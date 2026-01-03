import datetime
import json
import uuid

_VALUE = "v"
_TYPE = "$t"


class _CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return {_TYPE: "uuid", _VALUE: obj.hex}
        if isinstance(obj, datetime.datetime):
            return {_TYPE: "datetime", _VALUE: obj.timestamp()}
        return super().default(obj)


def _custom_decode(obj):
    if _TYPE not in obj:
        return obj
    t = obj.get(_TYPE)
    v = obj.get(_VALUE)
    if t == "uuid":
        return uuid.UUID(hex=v)
    if t == "datetime":
        return datetime.datetime.fromtimestamp(v)
    raise ValueError(f"unsupported {_TYPE} {t}")


def dumps(*args, **kwargs):
    return json.dumps(*args, **kwargs, cls=_CustomEncoder)


def loads(*args, **kwargs):
    return json.loads(*args, **kwargs, object_hook=_custom_decode)
