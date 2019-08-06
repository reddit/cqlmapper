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
import json
import logging
import os
import warnings

from collections import namedtuple

import six

from cassandra import metadata

from cqlmapper import columns
from cqlmapper import CQLEngineException
from cqlmapper.models import Model
from cqlmapper.usertype import UserType

CQLENG_ALLOW_SCHEMA_MANAGEMENT = "CQLENG_ALLOW_SCHEMA_MANAGEMENT"

Field = namedtuple("Field", ["name", "type"])

log = logging.getLogger(__name__)


def create_keyspace_simple(conn, name, replication_factor, durable_writes=True):
    """Creates a keyspace with SimpleStrategy for replica placement.

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production
    environments. Take care to execute schema modifications in a single
    context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an
    environment-driven conditional.*

    :param conn: Cassandra connection wrapper used to execute any CQL
        statements.
    :type: cqlengine.ConnectionInterface subclass
    :param name: name of keyspace to create
    :type: string
    :param replication_factor: keyspace replication factor, used with
        :attr:`~.SimpleStrategy`
    :type: int
    :param durable_writes: Write log is bypassed if set to False
    :type: bool
    """
    _create_keyspace(
        conn, name, durable_writes, "SimpleStrategy", {"replication_factor": replication_factor}
    )


def create_keyspace_network_topology(conn, name, dc_replication_map, durable_writes=True):
    """Creates a keyspace with NetworkTopologyStrategy for replica placement.

    If the keyspace already exists, it will not be modified.

    **This function should be used with caution, especially in production
    environments. Take care to execute schema modifications in a single
    context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an
    environment-driven conditional.*

    :param conn: Cassandra connection wrapper used to execute any CQL
        statements.
    :type: cqlengine.ConnectionInterface subclass
    :param name: name of keyspace to create
    :type: string
    :param dc_replication_map: map of dc_names: replication_factor
    :type: bool
    :param durable_writes: Write log is bypassed if set to False
    :type: bool
    :param connections: List of connection names
    :type: list
    """
    _create_keyspace(conn, name, durable_writes, "NetworkTopologyStrategy", dc_replication_map)


def _create_keyspace(conn, name, durable_writes, strategy_class, strategy_options):
    if not _allow_schema_modification():
        return

    cluster = conn.cluster

    if name not in cluster.metadata.keyspaces:
        log.info("Creating keyspace %s", name)
        ks_meta = metadata.KeyspaceMetadata(name, durable_writes, strategy_class, strategy_options)
        conn.execute(ks_meta.as_cql_query())
    else:
        log.info("Not creating keyspace %s because it already exists", name)


def drop_keyspace(conn, name):
    """Drops a keyspace, if it exists.

    *There are plans to guard schema-modifying functions with an
    environment-driven conditional.*

    **This function should be used with caution, especially in production
    environments. Take care to execute schema modifications in a single
    context (i.e. not concurrently with other clients).**

    :param conn: Cassandra connection wrapper used to execute any CQL
        statements.
    :type: cqlengine.ConnectionInterface subclass
    :param name: name of keyspace to drop
    :type: string
    """
    if not _allow_schema_modification():
        return

    if name not in conn.cluster.metadata.keyspaces:
        return

    conn.execute("DROP KEYSPACE {0}".format(metadata.protect_name(name)))


def _get_index_name_by_column(table, column_name):
    """Find the index name for a given table and column."""
    protected_name = metadata.protect_name(column_name)
    possible_index_values = [protected_name, "values(%s)" % protected_name]
    for index_metadata in table.indexes.values():
        options = dict(index_metadata.index_options)
        if options.get("target") in possible_index_values:
            return index_metadata.name


def sync_table(conn, model):
    """Inspects the model and creates / updates the corresponding table and
    columns.

    If `keyspaces` is specified, the table will be synched for all specified
    keyspaces. Note that the `Model.__keyspace__` is ignored in that case.

    If `connections` is specified, the table will be synched for all specified
    connections. Note that the `Model.__connection__` is ignored in that case.
    If not specified, it will try to get the connection from the Model.

    Any User Defined Types used in the table are implicitly synchronized.

    This function can only add fields that are not part of the primary key.

    Note that the attributes removed from the model are not deleted on the
    database. They become effectively ignored by (will not show up on) the
    model.

    :param conn: Cassandra connection wrapper used to execute any CQL
        statements.
    :type: cqlengine.ConnectionInterface subclass
    :param model: Model representing the table you want to sync.
    :type: cqlmapper.models.Model

    **This function should be used with caution, especially in production
    environments. Take care to execute schema modifications in a single
    context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an
    environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(model, Model):
        raise CQLEngineException("Models must be derived from base Model.")

    if model.__abstract__:
        raise CQLEngineException("cannot create table from abstract model")

    cluster = conn.cluster
    cf_name = model.column_family_name()
    raw_cf_name = model._raw_column_family_name()

    ks_name = conn.keyspace

    try:
        keyspace = cluster.metadata.keyspaces[ks_name]
    except KeyError:
        msg = "Keyspace '{0}' for model {1} does not exist."
        raise CQLEngineException(msg.format(ks_name, model))

    tables = keyspace.tables

    syncd_types = set()
    for col in model._columns.values():
        udts = []
        columns.resolve_udts(col, udts)
        for udt in [u for u in udts if u not in syncd_types]:
            _sync_type(conn, udt, syncd_types)

    if raw_cf_name not in tables:
        log.debug("sync_table creating new table %s in keyspace %s", cf_name, ks_name)
        qs = _get_create_table(model)

        try:
            conn.execute(qs)
        except CQLEngineException as ex:
            # 1.2 doesn't return cf names, so we have to examine the exception
            # and ignore if it says the column family already exists
            if "Cannot add already existing column family" not in unicode(ex):
                raise
    else:
        log.debug("sync_table checking existing table %s for keyspace %s", cf_name, keyspace)
        table_meta = tables[raw_cf_name]

        _validate_pk(model, table_meta)

        table_columns = table_meta.columns
        model_fields = set()

        for model_name, col in model._columns.items():
            db_name = col.db_field_name
            model_fields.add(db_name)
            if db_name in table_columns:
                col_meta = table_columns[db_name]
                if col_meta.cql_type != col.db_type:
                    msg = (
                        'Existing table {0} has column "{1}" with a type '
                        "({2}) differing from the model type ({3}). "
                        "Model should be updated."
                    )
                    msg = msg.format(cf_name, db_name, col_meta.cql_type, col.db_type)
                    warnings.warn(msg)
                    log.warning(msg)

                continue

            if col.primary_key or col.primary_key:
                msg = "Cannot add primary key '{0}' (with db_field '{1}') to " "existing table {2}"
                raise CQLEngineException(msg.format(model_name, db_name, cf_name))

            query = "ALTER TABLE {0} add {1}".format(cf_name, col.get_column_def())
            conn.execute(query)

        db_fields_not_in_model = model_fields.symmetric_difference(table_columns)
        if db_fields_not_in_model:
            msg = "Table {0} has fields not referenced by model: {1}"
            log.info(msg.format(cf_name, db_fields_not_in_model))

        _update_options(conn, model)

    table = cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]

    indexes = [c for n, c in model._columns.items() if c.index]

    # TODO: support multiple indexes in C* 3.0+
    for column in indexes:
        index_name = _get_index_name_by_column(table, column.db_field_name)
        if index_name:
            continue

        qs = ["CREATE INDEX"]
        qs += ["ON {0}".format(cf_name)]
        qs += ['("{0}")'.format(column.db_field_name)]
        qs = " ".join(qs)
        conn.execute(qs)


def _validate_pk(model, table_meta):
    model_partition = [c.db_field_name for c in model._partition_keys.values()]
    meta_partition = [c.name for c in table_meta.partition_key]
    model_clustering = [c.db_field_name for c in model._clustering_keys.values()]
    meta_clustering = [c.name for c in table_meta.clustering_key]

    if model_partition != meta_partition or model_clustering != meta_clustering:

        def _pk_string(partition, clustering):
            return "PRIMARY KEY (({0}){1})".format(
                ", ".join(partition), ", " + ", ".join(clustering) if clustering else ""
            )

        raise CQLEngineException(
            "Model {0} PRIMARY KEY composition does not match existing table {1}. "
            "Model: {2}; Table: {3}. "
            "Update model or drop the table.".format(
                model,
                model.column_family_name(),
                _pk_string(model_partition, model_clustering),
                _pk_string(meta_partition, meta_clustering),
            )
        )


def sync_type(conn, type_model):
    """
    Inspects the type_model and creates / updates the corresponding type.

    Note that the attributes removed from the type_model are not deleted on the database (this operation is not supported).
    They become effectively ignored by (will not show up on) the type_model.

    **This function should be used with caution, especially in production environments.
    Take care to execute schema modifications in a single context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    if not issubclass(type_model, UserType):
        raise CQLEngineException("Types must be derived from base UserType.")

    _sync_type(conn, type_model)


def _sync_type(conn, type_model, omit_subtypes=None):
    ks_name = conn.keyspace
    cluster = conn.cluster

    syncd_sub_types = omit_subtypes or set()
    for field in type_model._fields.values():
        udts = []
        columns.resolve_udts(field, udts)
        for udt in [u for u in udts if u not in syncd_sub_types]:
            _sync_type(conn, udt, syncd_sub_types)
            syncd_sub_types.add(udt)

    type_name = type_model.type_name()
    type_name_qualified = "{0}.{1}".format(ks_name, type_name)

    keyspace = cluster.metadata.keyspaces[ks_name]
    defined_types = keyspace.user_types
    if type_name not in defined_types:
        log.debug("sync_type creating new type %s", type_name_qualified)
        cql = get_create_type(type_model, ks_name)
        conn.execute(cql)
        conn.register_udt(type_model.type_name(), type_model)
    else:
        type_meta = defined_types[type_name]
        defined_fields = type_meta.field_names
        model_fields = set()
        for field in type_model._fields.values():
            model_fields.add(field.db_field_name)
            if field.db_field_name not in defined_fields:
                conn.execute(
                    "ALTER TYPE {0} ADD {1}".format(type_name_qualified, field.get_column_def())
                )
            else:
                field_type = type_meta.field_types[defined_fields.index(field.db_field_name)]
                if field_type != field.db_type:
                    msg = (
                        'Existing user type {0} has field "{1}" with a type ({2}) differing from the model user type ({3}).'
                        + "UserType should be updated."
                    )
                    msg = msg.format(
                        type_name_qualified, field.db_field_name, field_type, field.db_type
                    )
                    warnings.warn(msg)
                    log.warning(msg)
        conn.register_udt(type_model.type_name(), type_model)

        if len(defined_fields) == len(model_fields):
            log.info("Type %s did not require synchronization", type_name_qualified)
            return

        db_fields_not_in_model = model_fields.symmetric_difference(defined_fields)
        if db_fields_not_in_model:
            msg = "Type %s has fields not referenced by model: %s"
            log.info(msg, type_name_qualified, db_fields_not_in_model)


def get_create_type(type_model, keyspace):
    type_meta = metadata.UserType(
        keyspace,
        type_model.type_name(),
        (f.db_field_name for f in type_model._fields.values()),
        (v.db_type for v in type_model._fields.values()),
    )
    return type_meta.as_cql_query()


def _get_create_table(model):
    ks_table_name = model.column_family_name()
    query_strings = ["CREATE TABLE {0}".format(ks_table_name)]

    # add column types
    pkeys = []  # primary keys
    ckeys = []  # clustering keys
    qtypes = []  # field types

    def add_column(col):
        s = col.get_column_def()
        if col.primary_key:
            keys = pkeys if col.partition_key else ckeys
            keys.append('"{0}"'.format(col.db_field_name))
        qtypes.append(s)

    for name, col in model._columns.items():
        add_column(col)

    qtypes.append(
        "PRIMARY KEY (({0}){1})".format(", ".join(pkeys), ckeys and ", " + ", ".join(ckeys) or "")
    )

    query_strings += ["({0})".format(", ".join(qtypes))]

    property_strings = []

    _order = [
        '"{0}" {1}'.format(c.db_field_name, c.clustering_order or "ASC")
        for c in model._clustering_keys.values()
    ]
    if _order:
        property_strings.append("CLUSTERING ORDER BY ({0})".format(", ".join(_order)))

    # options strings use the V3 format, which matches CQL more closely and does not require mapping
    property_strings += metadata.TableMetadataV3._make_option_strings(model.__options__ or {})

    if property_strings:
        query_strings += ["WITH {0}".format(" AND ".join(property_strings))]

    return " ".join(query_strings)


def _get_table_metadata(conn, model):
    # returns the table as provided by the native driver for a given model
    ks = conn.keyspace
    table = model._raw_column_family_name()
    table = conn.cluster.metadata.keyspaces[ks].tables[table]
    return table


def _options_map_from_strings(option_strings):
    # converts options strings to a mapping to strings or dict
    options = {}
    for option in option_strings:
        name, value = option.split("=")
        i = value.find("{")
        if i >= 0:
            value = value[i : value.rfind("}") + 1].replace(
                "'", '"'
            )  # from cql single quotes to json double; not aware of any values that would be escaped right now
            value = json.loads(value)
        else:
            value = value.strip()
        options[name.strip()] = value
    return options


def _update_options(conn, model):
    """Updates the table options for the given model if necessary.

    :param Session conn: cassandra session
    :param model: The model to update.

    :return: `True`, if the options were modified in Cassandra,
        `False` otherwise.
    :rtype: bool
    """
    ks_name = conn.keyspace
    log.debug("Checking %s for option differences", model)
    model_options = model.__options__ or {}

    table_meta = _get_table_metadata(conn, model)
    # go to CQL string first to normalize meta from different versions
    existing_option_strings = set(table_meta._make_option_strings(table_meta.options))
    existing_options = _options_map_from_strings(existing_option_strings)
    model_option_strings = metadata.TableMetadataV3._make_option_strings(model_options)
    model_options = _options_map_from_strings(model_option_strings)

    update_options = {}
    for name, value in model_options.items():
        try:
            existing_value = existing_options[name]
        except KeyError:
            msg = "Invalid table option: '%s'; known options: %s"
            raise KeyError(msg % (name, existing_options.keys()))
        if isinstance(existing_value, six.string_types):
            if value != existing_value:
                update_options[name] = value
        else:
            try:
                for k, v in value.items():
                    if existing_value[k] != v:
                        update_options[name] = value
                        break
            except KeyError:
                update_options[name] = value

    if update_options:
        options = " AND ".join(metadata.TableMetadataV3._make_option_strings(update_options))
        query = "ALTER TABLE {0} WITH {1}".format(model.column_family_name(), options)
        conn.execute(query)
        return True

    return False


def drop_table(conn, model):
    """Drops the table indicated by the model, if it exists.

    :param conn: Cassandra connection wrapper used to execute any CQL
        statements.
    :type: cqlengine.ConnectionInterface subclass
    :param model: Model representing the table you want to drop.
    :type: cqlmapper.models.Model

    **This function should be used with caution, especially in production
    environments. Take care to execute schema modifications in a single
    context (i.e. not concurrently with other clients).**

    *There are plans to guard schema-modifying functions with an
    environment-driven conditional.*
    """
    if not _allow_schema_modification():
        return

    ks_name = conn.keyspace
    raw_cf_name = model._raw_column_family_name()

    try:
        conn.cluster.metadata.keyspaces[ks_name].tables[raw_cf_name]
        conn.execute("DROP TABLE {0};".format(model.column_family_name()))
    except KeyError:
        pass


def _allow_schema_modification():
    if not os.getenv(CQLENG_ALLOW_SCHEMA_MANAGEMENT):
        msg = (
            CQLENG_ALLOW_SCHEMA_MANAGEMENT
            + " environment variable is not set. Future versions of this package will require this variable to enable management functions."
        )
        warnings.warn(msg)
        log.warning(msg)

    return True
