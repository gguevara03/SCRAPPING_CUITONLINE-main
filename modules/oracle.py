import logging
from dataclasses import dataclass
from dataclasses import field

import awswrangler as wr
import pandas as pd
from sqlalchemy.types import VARCHAR

logger = logging.getLogger()

wr.oracle.oracledb.init_oracle_client()


@dataclass
class OracleDataInserter:
    con: wr.oracle.connect = field(init=False)
    schema: str
    glue_con: str
    call_timeout: int = field(default=30000)
    use_column_names: bool = field(default=True)

    def __post_init__(self):
        self.con = self.set_connection()

    def set_connection(self):
        con = wr.oracle.connect(connection=self.glue_con, call_timeout=self.call_timeout)
        return con

    def insert_data(self, df: pd.DataFrame, table_name: str, mode: str, primary_keys: list = None):
        wr.oracle.to_sql(
            df=df,
            table=table_name,
            schema=self.schema,
            con=self.con,
            mode=mode,
            use_column_names=self.use_column_names,
            dtype={c: VARCHAR(df[c].str.len().max()) for c in df.columns[df.dtypes == 'object'].tolist()},
            primary_keys=primary_keys
        )

    def ejecutar_consulta_sql(self, sql):
        data = wr.oracle.read_sql_query(sql=sql, con=self.con)
        return data