.. Comdb2 documentation master file, created by
   sphinx-quickstart on Thu Sep  8 16:16:10 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Comdb2's documentation!
==================================

Contents:

.. toctree::
   :maxdepth: 2

   quickstart
   good_practice
   cdb2
   dbapi2

Examples
========

.. code-block:: python

   #!/opt/bb/bin/python3.5
   
   import comdb2.dbapi2
   
   sql = """
   SELECT prc_comm_node_id, machine_name 
   FROM machine 
       INNER JOIN rhst_group 
       ON machine.cluster_id = rhst_group.group_number 
   WHERE group_name = 'XLNXPD'"""
   
   rhst3db = comdb2.dbapi2.connect("rhst3db")
   
   cursor = rhst3db.cursor()
   cursor.execute(sql)
   
   for r in cursor:
       print("{} {}".format(*r))

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
