import pandas as pd
import warnings
import paramiko
from datetime import date,time
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.engine import url as u
from dotenv import load_dotenv
load_dotenv()
from __init__ import *

warnings.filterwarnings('ignore')
rundate =  str(pd.to_datetime(date.today(), errors='coerce'))[:10]
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_rows', 1000)


class extractorFromQuery:
    def __init__(self, mode: str):
        self.mode = mode
        self.credentials = (
            {
                "sql_username": PROD_SQL_USERNAME,
                "sql_hostname": PROD_SQL_HOSTNAME,
                "sql_password": PROD_SQL_PASSWORD,
                "sql_main_database": PROD_SQL_MAIN_DATABASE,
            }
            if self.mode == "PL"
            else {
                "sql_username": CL_SQL_USERNAME,
                "sql_hostname": CL_SQL_HOSTNAME,
                "sql_password": CL_SQL_PASSWORD,
                "sql_main_database": CL_SQL_MAIN_DATABASE,
            }
        )
        self.key = paramiko.RSAKey.from_private_key_file(r"C:\Users\T14s\Desktop\aws\prod-redshift.pem")
        self.sql_port = SQL_PORT
        self.ssh_host = SSH_HOST
        self.ssh_user = SSH_USER
        self.ssh_port = SSH_PORT
        self.local_host = LOCAL_HOST

    def queryExecutor(self, query: str, *args):
        i = 0
        n_attemps = 5
        server = SSHTunnelForwarder(
            (self.ssh_host, self.ssh_port),
            ssh_username=self.ssh_user,
            ssh_pkey=self.key,
            remote_bind_address=(self.credentials["sql_hostname"], self.sql_port),
        )
        while i < n_attemps:
            try:
                server.start()
                url = u.URL(
                    drivername="postgresql+psycopg2",
                    username=self.credentials["sql_username"],
                    password=self.credentials["sql_password"],
                    host=self.local_host,
                    port=server.local_bind_port,
                    database=self.credentials["sql_main_database"],
                )
                conn = create_engine(url)
                result = pd.read_sql_query(query, conn, params=args)
                if result.shape[0] > 0:
                    break
            except Exception as e:
                i += 1
                logger.info(e.args)
                logger.info(f"restarting connection retry {i}")
                time.sleep(10)
        if i == 5:
            raise TimeoutError("Connection timed out")
        server.stop()
        return result

