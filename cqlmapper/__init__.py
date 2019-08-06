# Modifications Copyright 2016-2017 Reddit, Inc.
#
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import six

from cassandra.cluster import _NOT_SET


# Alias for the _NOT_SET singleton in cassandra.cluster
TIMEOUT_NOT_SET = _NOT_SET

# Caching constants.
CACHING_ALL = "ALL"
CACHING_KEYS_ONLY = "KEYS_ONLY"
CACHING_ROWS_ONLY = "ROWS_ONLY"
CACHING_NONE = "NONE"


class CQLEngineException(Exception):
    pass


class ValidationError(CQLEngineException):
    pass


class LWTException(CQLEngineException):
    """Lightweight conditional exception.

    This exception will be raised when a write using an `IF` clause could not
    be applied due to existing data violating the condition. The existing data
    is available through the ``existing`` attribute.

    :param existing: The current state of the data which prevented the write.
    """

    def __init__(self, existing):
        super(LWTException, self).__init__("LWT Query was not applied")
        self.existing = existing


class ConnectionInterface(object):
    """ Interface used to provide a unified Connection interface to cqlmapper.
    """

    def execute(self, query_or_statement, *a, **kw):
        raise NotImplementedError


class UnicodeMixin(object):
    if six.PY3:
        __str__ = lambda x: x.__unicode__()
    else:
        __str__ = lambda x: six.text_type(x).encode("utf-8")
