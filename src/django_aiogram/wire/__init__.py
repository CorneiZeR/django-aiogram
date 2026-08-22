"""Bytes on the queue, and back: everything about how a message is represented.

`serializers` turns a payload into bytes and back; `envelope` wraps one with the version
and correlation id a consumer needs to read it; `payloads` shapes and redacts what the
event log is allowed to keep.

None of it knows which transport carries the result, which is what lets the same envelope
travel a Redis list, a stream, a queue or a topic.
"""

#: deliberately empty: callers import from the modules in this package, not from the
#: package itself. A re-export here would make a second path to every name, and the one
#: nobody chose is the one that cannot be moved later
__all__: tuple[str, ...] = ()
