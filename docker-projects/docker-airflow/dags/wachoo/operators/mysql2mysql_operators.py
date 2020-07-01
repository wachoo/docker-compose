# -*- coding: utf-8 -*-
#

import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.operators.generic_transfer import GenericTransfer
from datetime import datetime
import sys

class MySQLToMySQLOperator(GenericTransfer):
    """
    Move mysql data to another mysql instance

    """

    @apply_defaults
    def __init__(
            self,
            sql,
            destination_table,
            source_conn_id,
            destination_conn_id,
            target_fields=None,
            db_name=None,
            *args, **kwargs):
        super(MySQLToMySQLOperator, self).__init__(
            sql=sql,
            destination_table=destination_table,
            source_conn_id=source_conn_id,
            destination_conn_id=destination_conn_id,
            *args, **kwargs)
        self.target_fields = target_fields
        self.db_name = db_name

    def execute(self, context):
        source_hook = BaseHook.get_hook(self.source_conn_id)

        logging.info("Extracting data from {}".format(self.source_conn_id))
        logging.info("Executing: \n" + self.sql.format(self.db_name))
        results = self.get_records(source_hook, self.sql.format(self.db_name))

        destination_hook = BaseHook.get_hook(self.destination_conn_id)

        logging.info("Inserting rows into {}".format(self.destination_conn_id))

        logging.info("results: {}".format(results[:5]))
        destination_hook.insert_rows(table=self.destination_table, rows=results, target_fields=self.target_fields)

    def get_records(self, source_hook, sql, parameters=None):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if sys.version_info[0] < 3:
            sql = sql.encode('utf-8')
        conn = source_hook.get_conn()
        cur = source_hook.get_cursor()
        cur.execute('SET SESSION group_concat_max_len=20000')
        if parameters is not None:
            cur.execute(sql, parameters)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
