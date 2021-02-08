cqlmapper
=========

This project is a fork of `cqlengine
<https://github.com/datastax/python-driver/tree/master/cassandra/cqlengine>`__
with a number of changes to allow it to work in a `baseplate
<https://github.com/reddit/baseplate>`__ application

Major changes:
    - Remove support for polymorphic models
    - Remove the query evaluator method of defining query constraints
    - Switch from using a global connection object to passing a connection object
      to methods that interact with the database
    - Batch queries are handled by a Connection-like Batch object that is given
      as the `conn` argument to functions rather than using the `Model.batch`
      syntax of cqlengine

Example usage::

    import uuid

    from cassandra.cluster import Cluster
    from cqlmapper import columns, connection, models
    from cqlmapper.batch import Batch
    from cqlmapper.connection import Connection as CQLMapperConnection
    from cqlmapper.management import sync_table

    class MyFirstModel(Model):
        id = columns.UUID(primary_key=True, default=uuid.uuid4)
        body = colums.Text()

    cluster = Cluster()
    session = cluster.connect("example")
    conn = CQLMapperConnection(session)
    sync_table(conn, MyFirstModel)
    model_1 = MyFirstModel.create(conn, body="Hello World")
    model_2 = MyFirstModel.create(conn, body="Hola Mundo")
    # Batch queries can be used as a context manager where the batch query will
    # be executed when exiting the context
    with Batch(conn) as batch_conn:
        MyFirstModel.create(batch_conn, body="Ciao mondo")
        MyFirstModel.create(batch_conn, body="Bonjour le monde")
    # Batch queries can also be created standalone in which case execute_batch
    # can be called to execute the queries.  Calls to execute will add the
    # query to the batch
    batch_conn = Batch(conn)
    MyFirstModel.create(batch_conn, body="Hallo Welt")
    MyFirstModel.create(batch_conn, body="Hei Verden")
    >>> MyFirstModel.objects.count(conn)
    4
    >>> batch_conn.execute_batch()
    >>> MyFirstModel.objects.count(conn)
    6
    >>> MyFirstModel.get(conn, id=model_1.id).text == model_1.text
    True
    >>> MyFirstModel.get(conn, id=model_2.id).text == model_1.text
    False

