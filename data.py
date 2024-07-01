import os
import datetime
import shutil
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
from sqlalchemy import create_engine
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logging.getLogger("snowflake.connector").setLevel(logging.WARNING)
# logging.getLogger('snowflake.connector.connection').propagate=False
logging.getLogger("snowflake.connector").propagate = False


class DataHandler:
    def __init__(self, runtype, source_data="DB", data_dir="data/raw", push_to_db=False, db="snowflake"):
        self.source_data = source_data
        self.data_dir = data_dir
        self.file_list = os.listdir(data_dir)
        self.push_to_db = push_to_db
        self.db = db
        if self.source_data == "DB":
            self.cursor = self.connect_snowflake()
        self.runtype = runtype
        self.store_dir = "data"
        if self.db == "mysql":
            self.engine = create_engine(
                "mysql+mysqldb://e3user:" + "linkmatch123" + "@localhost/link_ml"
            )
        if self.store_dir not in os.listdir("."):
            os.makedirs(self.store_dir)
        self.store_dir = "data/"

    def make_store1_unique(self, df, id_col):
        df1 = df.drop_duplicates(id_col).reset_index(drop=True)
        return df1

    def make_store2_unique(self, df, id_col):
        df1 = df.drop_duplicates(id_col).reset_index(drop=True)
        return df1

    def connect_snowflake(self):
        try:
            ctx = snowflake.connector.connect(
                user="aayyar",
                password="R42mcse001Xg6s3V",
                account="en35112.east-us-2.azure",
                database="E3_PROD",
                schema="link",
            )
            cs = ctx.cursor()
            cursor = cs.execute("USE SCHEMA link")
            cursor = cs.execute("use role santafe")
            logging.info("Snowflake connected")
        except:
            logging.info("Snowflake not connected")
        return cursor

    def push_metrics_to_db(self, df, table):
        if self.push_to_db:
            if self.db == "snowflake":
                ctx = snowflake.connector.connect(
                    user="aayyar",
                    password="R42mcse001Xg6s3V",
                    account="en35112.east-us-2.azure",
                    database="E3_PROD",
                    schema="link",
                )
                cs = ctx.cursor()
                cursor = cs.execute("use role santafe")
                cursor = cs.execute("USE SCHEMA link_ml")
                write_pandas(ctx, df, table)
            elif self.db == "mysql":
                df.to_sql(
                    table,
                    self.engine,
                    index=False,
                    if_exists="append",
                    chunksize=500,
                    method="multi",
                )

    def push_links_to_db(self, df):
        if self.push_to_db:
            ctx = snowflake.connector.connect(
                user="aayyar",
                password="R42mcse001Xg6s3V",
                account="en35112.east-us-2.azure",
                database="E3_PROD",
                schema="link",
            )
            cs = ctx.cursor()
            cursor = cs.execute("use role santafe")
            cursor = cs.execute("USE SCHEMA link")
            write_pandas(ctx, df, "STAGE_PROPOSED_AUTOMATED_LINKS")

    def get_comp_name_db(self, run_id, client_name):
        query_exec = f"select COMP_BANNER_NAME from SUMMARY where RUN_ID='{run_id}' and CLIENT_NAME='{client_name}'"
        print(query_exec)
        df = pd.DataFrame()
        if self.db == "snowflake":
            ctx = snowflake.connector.connect(
                user="aayyar",
                password="R42mcse001Xg6s3V",
                account="en35112.east-us-2.azure",
                database="E3_PROD",
                schema="link",
            )
            cs = ctx.cursor()
            cursor = cs.execute("use role santafe")
            cursor = cs.execute("USE SCHEMA link_ml")
            cursor.execute(query_exec)
            dat = cursor.fetchmany(100)
            df = pd.DataFrame(dat, columns=["COMP_BANNER_NAME"])
        elif self.db == "mysql":
            with self.engine.connect() as con:
                df = pd.read_sql(text(query_exec), con)
            # run_ids=df['RUN_ID'].tolist()
        return df

    def get_client_name_db(self, run_id):
        query_exec = (
            f"select CLIENT_NAME from link_ml.COMPLETED where RUN_ID='{run_id}'"
        )
        print(query_exec)
        df = pd.DataFrame()
        if self.db == "snowflake":
            ctx = snowflake.connector.connect(
                user="aayyar",
                password="R42mcse001Xg6s3V",
                account="en35112.east-us-2.azure",
                database="E3_PROD",
                schema="link",
            )
            cs = ctx.cursor()
            cursor = cs.execute("use role santafe")
            cursor = cs.execute("USE SCHEMA link_ml")
            cursor.execute(query_exec)
            dat = cursor.fetchmany(100)
            df = pd.DataFrame(dat, columns=["CLIENT_NAME"])
        elif self.db == "mysql":
            with self.engine.connect() as con:
                df = pd.read_sql(text(query_exec), con)
            # run_ids=df['RUN_ID'].tolist()
        return df
    
    def get_ml_config_data(self):
        #print('reached here')
        #cursor=self.connect_snowflake()
        query='select * from link.ml_run_config'
        self.cursor.execute(query)
        dat = self.cursor.fetchmany(10000)
        df = pd.DataFrame(dat, columns=["CLIENT_ID","COMP_BANNER_ID","PSWAP_PROJECT_ID","PROJECT_DESCR","CLIENT_NAME"])
        #print(df)
        return df

    def load_client_data_from_file(self, fname, ml_config_name, data_dir):
        if (fname in os.listdir(data_dir)) and (ml_config_name in os.listdir(data_dir)):
            df = pd.read_csv(data_dir + fname)
            df = df.dropna().reset_index(drop=True)
            dfs = pd.read_csv(data_dir + ml_config_name)
            logging.info(f"Number of clients in the file : {len(df)}")
        else:
            logging.info(f"{fname} not present in current directory")
            df = pd.DataFrame()
            dfs = pd.DataFrame()

        return df, dfs
    
    def load_client_data(self, source_table, fname, ml_config_name, data_dir):

        if source_table=='link.ml_run_config':
            ml_config_data=self.get_ml_config_data()
            client_dfs=ml_config_data[['CLIENT_ID','CLIENT_NAME','PROJECT_DESCR','COMP_BANNER_ID']].drop_duplicates(subset=['CLIENT_NAME','PROJECT_DESCR','COMP_BANNER_ID'])
            client_df1=client_dfs.drop_duplicates('CLIENT_ID',keep='first')
            all_clients=self.get_all_clients_data()
            all_clients=all_clients[['CLIENT_ID','VERTICAL']]
            client_df=client_df1.merge(all_clients,left_on="CLIENT_ID",right_on="CLIENT_ID")
            client_df['PROJECT NAME']=client_df['PROJECT_DESCR']
            miss_df=client_df1[~client_df1['CLIENT_NAME'].isin(client_df['CLIENT_NAME'].values.tolist())]
            miss_df.to_csv('data/static/missing_df.csv')
            client_dfs=client_dfs.dropna()
            client_dfs['COMP_BANNER_ID']=client_dfs['COMP_BANNER_ID'].astype('int')
            client_df=client_df[['CLIENT_ID','CLIENT_NAME','VERTICAL','PROJECT NAME']].reset_index(drop=True)
        else:
            client_df, client_dfs=self.load_client_data_from_file(fname, ml_config_name, data_dir)

        client_df=client_df[~client_df['VERTICAL'].str.contains('Grocery')].reset_index(drop=True)

        return client_df, client_dfs

    def get_client_id(self, client_name):
        query = (
            "select client_id,name from client.e3_client where name="
            + "'"
            + client_name
            + "';"
        )
        self.cursor.execute(query)
        dat = self.cursor.fetchmany(10)
        df = pd.DataFrame(dat, columns=["client_id", "name"])
        return df["client_id"].tolist()[0]

    def test_client_name(self, client_name):
        query = (
            "select client_id,name from client.e3_client where name="
            + "'"
            + client_name
            + "';"
        )
        self.cursor.execute(query)
        dat = self.cursor.fetchmany(1)
        df = pd.DataFrame(dat, columns=["client_id", "name"])
        if len(df) == 1:
            return True
        else:
            return False

    def fetch_data(self, store_dir, fname):
        cols = []
        list_dir = "temp"
        if list_dir not in os.listdir("."):
            os.makedirs(list_dir)
        for c in self.cursor.description:
            cols.append(c[0])
        # results_df=pd.DataFrame()
        chunk = 1
        if fname not in os.listdir(store_dir):
            while True:
                dat = self.cursor.fetchmany(50000)
                if not dat:
                    break
                df = pd.DataFrame(dat, columns=cols)
                df.to_csv("temp/chunk-" + str(chunk) + ".csv", index=None)
                logging.info(f"Wrote {chunk} to temp {len(df)}")
                chunk += 1
            logging.info(f"Wrote {chunk-1} files to temp")

            CHUNK_SIZE = 50000
            csv_file_list = os.listdir("temp/")
            output_file = store_dir + fname
            logging.info(csv_file_list)
            first_one = True
            for csv_file_name in csv_file_list:

                if (
                    not first_one
                ):  # if it is not the first csv file then skip the header row (row 0) of that file
                    skip_row = [0]
                else:
                    skip_row = []

                chunk_container = pd.read_csv(
                    "temp/" + csv_file_name, chunksize=CHUNK_SIZE, skiprows=skip_row
                )
                for chunks in chunk_container:
                    chunks.to_csv(output_file, mode="a", index=False)
                first_one = False
        else:
            chunk = 2
        shutil.rmtree(list_dir)

    def get_comp_list_from_db(self, client_name, client_items):

        comps_unlinked_all = (
            "select pl.comp_banner_name,pl.comp_banner_id,count(*) as cnt \
        from link.potential_links pl \
        left join price.ret_product_client_price rpc on pl.ret_product_id = rpc.ret_product_id \
        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
        where pl.project_active_flag \
        and pl.client_name ="+ "'"+ client_name+ "'"+ "  \
        and (pl.link_type is null or pl.link_type in ('NOMATCH') or pl.link_status in ('REJECTED','INVALID')) \
        group by pl.comp_banner_name,pl.comp_banner_id \
        order by cnt desc;"
        )

        comps_unlinked_others = (
            "select pl.comp_banner_name,pl.comp_banner_id,count(*) as cnt \
        from link.potential_links pl \
        left join price.ret_product_client_price rpc on pl.ret_product_id = rpc.ret_product_id \
        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
        where pl.project_active_flag \
        and pl.project_descr="+"'"+ client_items+ "'"+"  \
        and pl.client_name ="+ "'"+ client_name+ "'"+ "  \
        and (pl.link_type is null or pl.link_type in ('NOMATCH','REJECTED','INVALID')) \
        group by pl.comp_banner_name,pl.comp_banner_id \
        order by cnt desc;"
        )

        comp_stale_all = (
            'with stale as ( \
        select PSWAP_MEMBER_ID, COMP_BANNER_ID,\
            max(coalesce(pcss.product_store_count, 0)) max_product_store_count,\
            max(pcss.total_store_set_count) max_total_store_set_count,\
            min(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0) < coalesce(st.stale_threshold, 10))  stale\
        from link.links l\
                inner join link.project_store_set pss\
                            on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID\
                left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id\
                            and level = 0 \
                            and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                            and pcss.ret_product_id = l.comp_ret_product_id \
                left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                left join ( \
                        select cs.client_id, max(parse_json(JSON_VAL):"STALE-THRESHOLD")::numeric stale_threshold,\
                                        max(parse_json(JSON_VAL):"WARNING-THRESHOLD")::numeric warning_threshold\
                        from client.client_setting cs \
                        inner join client.e3_client c on cs.client_id = c.client_id\
                        where type = '+ "'"+ "PRODUCT_LINKING"+ "'"+ "\
                        and JSON_VAL like '%THRE%'\
                        and c.name ="+ "'"+ client_name+ "'"+ "\
                        group by cs.client_id \
                ) st on l.client_id = st.client_id\
                where l.project_active_flag \
                    and l.client_name ="+ "'"+ client_name+ "'"+ " \
                group by PSWAP_MEMBER_ID, COMP_BANNER_ID \
                having stale \
            ) \
            select l.comp_banner_id,l.comp_banner_name,count(*) as cnt\
                from link.links l \
                    inner join stale on l.PSWAP_MEMBER_ID = stale.PSWAP_MEMBER_ID and l.COMP_BANNER_ID = stale.COMP_BANNER_ID \
                    inner join link.project_store_set pss on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                    left join store.ret_banner b on b.banner_id=l.banner_id \
                    left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                        and level = 0 \
                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                        and pcss.ret_product_id = l.comp_ret_product_id \
                    left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                    left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                where l.project_active_flag \
                and l.client_name ="+ "'"+ client_name+ "'"+ " \
            and coalesce(pcss.product_store_count, 0) = 0\
            and l.link_status ='APPROVED' \
            group by 1,2;"
        )

        comp_stale = (
            'with stale as ( \
        select PSWAP_MEMBER_ID, COMP_BANNER_ID,\
            max(coalesce(pcss.product_store_count, 0)) max_product_store_count,\
            max(pcss.total_store_set_count) max_total_store_set_count,\
            min(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0) < coalesce(st.stale_threshold, 10))  stale\
        from link.links l\
                inner join link.project_store_set pss\
                            on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID\
                left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id\
                            and level = 0 \
                            and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                            and pcss.ret_product_id = l.comp_ret_product_id \
                left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                left join ( \
                        select cs.client_id, max(parse_json(JSON_VAL):"STALE-THRESHOLD")::numeric stale_threshold,\
                                        max(parse_json(JSON_VAL):"WARNING-THRESHOLD")::numeric warning_threshold\
                        from client.client_setting cs \
                        inner join client.e3_client c on cs.client_id = c.client_id\
                        where type = '+ "'"+ "PRODUCT_LINKING"+ "'"+ "\
                        and JSON_VAL like '%THRE%'\
                        and c.name ="+ "'"+ client_name+ "'"+ "\
                        group by cs.client_id \
                ) st on l.client_id = st.client_id\
                where l.project_active_flag \
                    and l.client_name ="+ "'"+ client_name+ "'"+ " \
                group by PSWAP_MEMBER_ID, COMP_BANNER_ID \
                having stale \
            ) \
            select l.comp_banner_id,l.comp_banner_name,count(*) as cnt\
                from link.links l \
                    inner join stale on l.PSWAP_MEMBER_ID = stale.PSWAP_MEMBER_ID and l.COMP_BANNER_ID = stale.COMP_BANNER_ID \
                    inner join link.project_store_set pss on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                    left join store.ret_banner b on b.banner_id=l.banner_id \
                    left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                        and level = 0 \
                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                        and pcss.ret_product_id = l.comp_ret_product_id \
                    left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                    left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                where l.project_active_flag \
                and l.project_descr =" + "'" + client_items+ "'"+ " \
                and l.client_name ="+ "'"+ client_name + "'" + " \
            and coalesce(pcss.product_store_count, 0) = 0\
            and l.link_status ='APPROVED' \
            group by 1,2;"
        )

        if self.runtype == "unlinked":
            if client_items == "Misc":
                comp_exec = comps_unlinked_all
                logging.info("choosing comps_unlinked_all")
            else:
                comp_exec = comps_unlinked_others
                logging.info("choosing comps_unlinked_others")
                logging.info(f"{comp_exec}")
        elif self.runtype == "stale":
            if client_items == "Misc":
                comp_exec = comp_stale_all
            else:
                comp_exec = comp_stale
            logging.info("choosing comps_stale")
        else:
            logging.info("Invalid Runtype")

        if "comp_list.csv" in os.listdir("data/"):
            os.remove("data/comp_list.csv")
        self.cursor.execute(comp_exec)
        download_file='comp_list.csv'
        comps_df=pd.DataFrame()
        comp_ids=[]
        data=self.fetch_data(self.store_dir,download_file)
        logging.info('Comp fetches')
        if download_file in os.listdir(self.store_dir):
            comps_df = pd.read_csv(self.store_dir + download_file)

        if len(comps_df) > 1:
            comp_ids = tuple(comps_df["COMP_BANNER_ID"].tolist())
        elif len(comps_df) == 1:
            comp_ids = comps_df["COMP_BANNER_ID"].tolist()[0]
        return comp_ids, comps_df
    
    def get_comp_list(self, client_name, client_items):
        if self.source_data == 'DB':
            _, comp_df = self.get_comp_list_from_db(client_name, client_items)
            if 'comp_list.csv' in os.listdir('data/'):
                os.remove('data/comp_list.csv')
        elif self.source_data == 'file':
            client_name = client_name.lower().replace("'", "")
            comp_file_name='comps-'+client_name+'.csv'
            if comp_file_name in self.file_list:
                comp_df = pd.read_csv(self.data_dir+comp_file_name)
            else:
                comp_df = pd.DataFrame()
        return comp_df

    def get_client_data_from_db(self, client_name, client_items):
        lhs_unlinked_others = (
            "select pl.product_set_member_id,pl.size_uofm,pl.total_size_uofm,pl.ret_product_id,rpc.client_reg_price,pl.upc,pl.brand,pl.description,rpc.sales,pl.comp_banner_name,pl.comp_banner_id,ps.shop_description,ps.l1_name,ps.l2_name,ps.l3_name,pl.pswap_member_id,pl.client_name,b.banner_name \
        from link.potential_links pl \
        left join price.ret_product_client_price rpc on pl.ret_product_id = rpc.ret_product_id \
        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
        left join store.ret_banner b on b.banner_id=pl.banner_id \
        where pl.project_active_flag \
        and pl.project_descr="+ "'"+ client_items+ "'" + "  \
        and pl.client_name ="+ "'"+ client_name + "'"+ "  \
        and (pl.link_type is null or pl.link_type in ('NOMATCH') or pl.link_status in ('REJECTED','INVALID')) \
        order by pl.ret_product_id desc nulls last;  \
        "
        )

        lhs_unlinked_all = (
            "select pl.product_set_member_id,pl.size_uofm,pl.total_size_uofm,pl.ret_product_id,rpc.client_reg_price,pl.upc,pl.brand,pl.description,rpc.sales,pl.comp_banner_name,pl.comp_banner_id,ps.shop_description,ps.l1_name,ps.l2_name,ps.l3_name,pl.client_name,b.banner_name \
        from link.potential_links pl \
        left join price.ret_product_client_price rpc on pl.ret_product_id = rpc.ret_product_id \
        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
        left join store.ret_banner b on b.banner_id=pl.banner_id \
        where pl.project_active_flag \
        and pl.client_name =" + "'" + client_name + "'"+ "  \
        and (pl.link_type is null or pl.link_type in ('NOMATCH') or pl.link_status in ('REJECTED','INVALID')) \
        order by pl.ret_product_id desc nulls last;"
        )

        lhs_stale = (
            'with stale as ( \
        select PSWAP_MEMBER_ID, COMP_BANNER_ID,\
            max(coalesce(pcss.product_store_count, 0)) max_product_store_count,\
            max(pcss.total_store_set_count) max_total_store_set_count,\
            min(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0) < coalesce(st.stale_threshold, 10))  stale\
        from link.links l\
                inner join link.project_store_set pss\
                            on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID\
                left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id\
                            and level = 0 \
                            and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                            and pcss.ret_product_id = l.comp_ret_product_id \
                left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                left join ( \
                        select cs.client_id, max(parse_json(JSON_VAL):"STALE-THRESHOLD")::numeric stale_threshold,\
                                        max(parse_json(JSON_VAL):"WARNING-THRESHOLD")::numeric warning_threshold\
                        from client.client_setting cs \
                        inner join client.e3_client c on cs.client_id = c.client_id\
                        where type = '+ "'" + "PRODUCT_LINKING" + "'"+ "\
                        and JSON_VAL like '%THRE%'\
                        and c.name =" + "'"+ client_name+ "'"+ "\
                        group by cs.client_id \
                ) st on l.client_id = st.client_id\
                where l.project_active_flag \
                    and l.client_name ="+ "'" + client_name + "'" + " \
                group by PSWAP_MEMBER_ID, COMP_BANNER_ID \
                having stale \
            ) \
            select to_char(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0), '990.99') || ' %' as coverage, \
            l.ret_product_id,rpc.client_reg_price,rpc.sales,l.product_set_member_id,l.brand,l.description,l.upc,l.size_uofm, \
            l.client_total_size,l.client_total_uom,l.comp_banner_id,l.comp_banner_name,ps.shop_description, \
            l.comp_description,l.comp_ret_product_id,l.pswap_member_id,l.client_name,l.l1_name,l.l2_name,l.l3_name,l.sku,l.comp_sku,l.banner_id,b.banner_name \
                from link.links l \
                    inner join stale on l.PSWAP_MEMBER_ID = stale.PSWAP_MEMBER_ID and l.COMP_BANNER_ID = stale.COMP_BANNER_ID \
                    inner join link.project_store_set pss on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                    left join store.ret_banner b on b.banner_id=l.banner_id \
                    left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                        and level = 0 \
                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                        and pcss.ret_product_id = l.comp_ret_product_id \
                    left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                    left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                where l.project_active_flag \
                and l.project_descr =" + "'" + client_items + "'" + " \
                and l.client_name =" + "'"+ client_name + "'" + " \
            and coalesce(pcss.product_store_count, 0) = 0 \
            and l.link_status ='APPROVED' \
            order by rpc.sales desc nulls last;"
        )

        lhs_stale_all = (
            'with stale as ( \
        select PSWAP_MEMBER_ID, COMP_BANNER_ID,\
            max(coalesce(pcss.product_store_count, 0)) max_product_store_count,\
            max(pcss.total_store_set_count) max_total_store_set_count,\
            min(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0) < coalesce(st.stale_threshold, 10))  stale\
        from link.links l\
                inner join link.project_store_set pss\
                            on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID\
                left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id\
                            and level = 0 \
                            and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                            and pcss.ret_product_id = l.comp_ret_product_id \
                left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                left join ( \
                        select cs.client_id, max(parse_json(JSON_VAL):"STALE-THRESHOLD")::numeric stale_threshold,\
                                        max(parse_json(JSON_VAL):"WARNING-THRESHOLD")::numeric warning_threshold\
                        from client.client_setting cs \
                        inner join client.e3_client c on cs.client_id = c.client_id\
                        where type = '+ "'"+ "PRODUCT_LINKING"+ "'"+ "\
                        and JSON_VAL like '%THRE%'\
                        and c.name =" + "'"+ client_name+ "'"+ "\
                        group by cs.client_id \
                ) st on l.client_id = st.client_id\
                where l.project_active_flag \
                    and l.client_name ="+ "'"+ client_name+ "'"+ " \
                group by PSWAP_MEMBER_ID, COMP_BANNER_ID \
                having stale \
            ) \
            select to_char(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0), '990.99') || ' %' as coverage, \
            l.ret_product_id,rpc.client_reg_price,rpc.sales,l.product_set_member_id,l.brand,l.description,l.upc,l.size_uofm, \
            l.client_total_size,l.client_total_uom,l.comp_banner_id,l.comp_banner_name,ps.shop_description, \
            l.comp_description,l.comp_ret_product_id,l.pswap_member_id,l.client_name,l.l1_name,l.l2_name,l.l3_name,l.sku,l.comp_sku,l.banner_id,b.banner_name \
                from link.links l \
                    inner join stale on l.PSWAP_MEMBER_ID = stale.PSWAP_MEMBER_ID and l.COMP_BANNER_ID = stale.COMP_BANNER_ID \
                    inner join link.project_store_set pss on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                    left join store.ret_banner b on b.banner_id=l.banner_id \
                    left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                        and level = 0 \
                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                        and pcss.ret_product_id = l.comp_ret_product_id \
                    left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                    left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                where l.project_active_flag \
                and l.client_name ="+ "'" + client_name + "'"+ " \
            and coalesce(pcss.product_store_count, 0) = 0 \
            and l.link_status ='APPROVED' \
            order by rpc.sales desc nulls last;"
        )

        if self.runtype == "unlinked":
            if client_items == "Misc":
                query_exec = lhs_unlinked_all

            else:
                query_exec = lhs_unlinked_others
        elif self.runtype == "stale":
            if client_items == "Misc":
                query_exec = lhs_stale_all
            else:
                query_exec = lhs_stale
        else:
            print("Invalid Runtype")
        logging.info(query_exec)
        if "store1_data.csv" in os.listdir("data/"):
            os.remove("data/store1_data.csv")

        store_df = pd.DataFrame()
        self.cursor.execute(query_exec)
        download_file = "store1_data.csv"
        data = self.fetch_data(self.store_dir, download_file)
        print("LHS fetches")
        if download_file in os.listdir(self.store_dir):
            store_df = pd.read_csv(self.store_dir + download_file)
        return store_df
    
    def get_client_data(self, client_name, client_items):

        if "'" in client_name:
            client_name_split=client_name.split("'")
            client_name =client_name_split[0]+"\\"+"'"+client_name_split[1]

        if self.source_data == 'DB':
            df = self.get_client_data_from_db(client_name, client_items)
            if 'store1_data.csv' in os.listdir('data/'):
                os.remove('data/store1_data.csv')
        
        elif self.source_data == 'file':
            fname = "store1-"+client_name+".csv"
            if fname in self.file_list:
                df = pd.read_csv(self.data_dir+fname)
            else:
                df = pd.DataFrame()
        
        return df

    def get_comp_data_one(self, client_name, comp_id):

        rhs_unlinked = (
            "select b.banner_name,b.banner_id,rp.brand,rp.description,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url \
        from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
            from link.project_store_set \
            ) pss \
        inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and level = 0 and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
        inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
        join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
        inner join store.ret_banner b on rp.banner_id = b.banner_id \
        where pcss.crawl_rp_flag \
        and client_name like "
            + "'"
            + client_name
            + "'"
            + " \
        and b.banner_id ="
            + str(comp_id)
        )

        rhs_stale = (
            "select b.banner_name,b.banner_id,rp.brand,rp.description,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url \
        from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
            from link.project_store_set \
            ) pss \
        inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and level = 0 and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
        inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
        join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
        inner join store.ret_banner b on rp.banner_id = b.banner_id \
        where pcss.crawl_rp_flag \
        and client_name like "
            + "'"
            + client_name
            + "'"
            + " \
        and b.banner_id ="
            + str(comp_id)
        )

        if self.runtype == "unlinked":
            query_exec = rhs_unlinked
        elif self.runtype == "stale":
            query_exec = rhs_stale
        else:
            print("Invalid Runtype")

        store_df = pd.DataFrame()
        if "store2_data.csv" in os.listdir("data/"):
            os.remove("data/store2_data.csv")
        self.cursor.execute(query_exec)
        download_file = "store2_data.csv"
        data = self.fetch_data(self.store_dir, download_file)
        print("RHS fetches")
        if download_file in os.listdir(self.store_dir):
            store_df = pd.read_csv(self.store_dir + download_file)
        return store_df

    def get_comp_data_full(self, client_name, comp_ids):

        rhs_unlinked = (
            "select b.banner_name,b.banner_id,rp.brand,rp.description,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url \
        from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
            from link.project_store_set \
            ) pss \
        inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and level = 0 and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
        inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
        join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
        inner join store.ret_banner b on rp.banner_id = b.banner_id \
        where pcss.crawl_rp_flag \
        and client_name like "
            + "'"
            + client_name
            + "'"
            + " \
        and b.banner_id in {};".format(
                comp_ids
            )
        )

        rhs_unlinked_1 = (
            "select b.banner_name,b.banner_id,rp.brand,rp.description,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url \
        from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
            from link.project_store_set \
            ) pss \
        inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and level = 0 and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
        inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
        join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
        inner join store.ret_banner b on rp.banner_id = b.banner_id \
        where pcss.crawl_rp_flag \
        and client_name like "
            + "'"
            + client_name
            + "'"
            + " \
        and b.banner_id = "
            + str(comp_ids)
        )

        rhs_stale = (
            "select b.banner_name,b.banner_id,rp.brand,rp.description,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url \
        from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
            from link.project_store_set \
            ) pss \
        inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and level = 0 and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
        inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
        join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
        inner join store.ret_banner b on rp.banner_id = b.banner_id \
        where pcss.crawl_rp_flag \
        and client_name like "
            + "'"
            + client_name
            + "'"
            + " \
        and b.banner_id in {};".format(
                comp_ids
            )
        )

        rhs_stale_1 = (
            "select b.banner_name,b.banner_id,rp.brand,rp.description,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url \
        from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
            from link.project_store_set \
            ) pss \
        inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and level = 0 and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
        inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
        join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
        inner join store.ret_banner b on rp.banner_id = b.banner_id \
        where pcss.crawl_rp_flag \
        and client_name like "
            + "'"
            + client_name
            + "'"
            + " \
        and b.banner_id = "
            + str(comp_ids)
        )

        if self.runtype == "unlinked":
            if len(comp_ids) == 1:
                query_exec = rhs_unlinked_1
            else:
                query_exec = rhs_unlinked
        elif self.runtype == "stale":
            if len(comp_ids) == 1:
                query_exec = rhs_stale_1
            else:
                print(rhs_stale)
                query_exec = rhs_stale
        else:
            print("Invalid Runtype")

        if "store2_data.csv" in os.listdir("data/"):
            os.remove("data/store2_data.csv")
        self.cursor.execute(query_exec)
        download_file = "store2_data.csv"
        data = self.fetch_data(self.store_dir, download_file)
        print("RHS fetches")
        if download_file in os.listdir(self.store_dir):
            store_df = pd.read_csv(self.store_dir + download_file)
        return store_df
    
    def get_comp_data_from_db(self, client_name, comp_id):

        rhs_unlinked= (
            "with comp_psm as ( \
            select crawl_psm.*, \
            row_number() over (partition by crawl_psm.RET_PRODUCT_ID \
            order by (case when length(crawl_psm.DESCRIPTION) between 5 and 300 then 0 else 1 end), \
            crawl_psm.created_dt desc)    rn \
            from product.product_set_member crawl_psm \
            inner join product.product_set crawl_ps on crawl_ps.product_set_id = crawl_psm.product_set_id \
            inner join (select 'Crawl PSM Collector : ' || pc.price_collector_id set_name \
                        from mission.price_collector pc \
                        where price_collector_name like 'Online%' \
                        and price_collector_name not like 'Online: E3 Internal Synthetic Data%' \
                        group by 1 \
                        ) sn on crawl_ps.SET_NAME = sn.set_name \
                        where crawl_psm.URL is not null \
                        qualify rn = 1 \
                ), \
            rph_mph_flat as (\
                    select rph_flat.rph_node_id,\
                    rph_flat.l1_name, rph_flat.l2_name, rph_flat.l3_name, rph_flat.l4_name, rph_flat.l5_name, \
                    mph.mph_node_id, mph.mph_node_name, mph.l1_name mph_l1_name, mph.l2_name mph_l2_name, \
                    mph.l3_name mph_l3_name, mph.l4_name mph_l4_name, mph.l5_name mph_l5_name \
                    from product.rph_flat \
                    left join product.rph_mph_xref x on rph_flat.rph_node_id = x.rph_node_id \
                    left join product.v_mph_flat mph on x.mph_node_id = mph.mph_node_id \
                        ), \
            rph_product_xref as ( \
                        select xref.* \
                        from product.rph_product_xref xref \
                        inner join product.ret_product rp on xref.ret_product_id = rp.ret_product_id \
                        qualify row_number() over(partition by xref.ret_product_id order by xref.last_seen desc) = 1 \
            ) \
            select b.banner_name,b.banner_id,rp.brand,rp.description,rp.mst_product_id,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url, \
            comp_psm.l1_name comp_l1_name, \
            comp_psm.l2_name comp_l2_name, \
            comp_psm.l3_name comp_l3_name,\
            comp_psm.l4_name comp_l4_name, \
            comp_psm.l5_name comp_l5_name,rpaxf.ATTR_VALUE client_attr_value \
            from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
                from link.project_store_set \
                ) pss \
            inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
            inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
            left join  comp_psm on rp.ret_product_id = comp_psm.ret_product_id \
            left join product.RET_PRODUCT_ATTRIBUTE_XREF_FLAT rpaxf on rp.RET_PRODUCT_ID = rpaxf.RET_PRODUCT_ID \
            join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
            inner join store.ret_banner b on rp.banner_id = b.banner_id \
            where client_name="+"'"+client_name+"'"+" \
            and rpaxf.CLIENT_ID = '9215ba4e-bcb8-4ad6-9f0d-fd00a71e83fc' \
            and rp.replaced_by is null \
            and b.banner_id =" + str(comp_id)
        )

        rhs_stale = rhs_unlinked

        rhs_sams= (
            "with comp_psm as ( \
            select crawl_psm.*, \
            row_number() over (partition by crawl_psm.RET_PRODUCT_ID \
            order by (case when length(crawl_psm.DESCRIPTION) between 5 and 300 then 0 else 1 end), \
            crawl_psm.created_dt desc)    rn \
            from product.product_set_member crawl_psm \
            inner join product.product_set crawl_ps on crawl_ps.product_set_id = crawl_psm.product_set_id \
            inner join (select 'Crawl PSM Collector : ' || pc.price_collector_id set_name \
                        from mission.price_collector pc \
                        where price_collector_name like 'Online%' \
                        and price_collector_name not like 'Online: E3 Internal Synthetic Data%' \
                        group by 1 \
                        ) sn on crawl_ps.SET_NAME = sn.set_name \
                        where crawl_psm.URL is not null \
                        qualify rn = 1 \
                ), \
            rph_mph_flat as (\
                    select rph_flat.rph_node_id,\
                    rph_flat.l1_name, rph_flat.l2_name, rph_flat.l3_name, rph_flat.l4_name, rph_flat.l5_name, \
                    mph.mph_node_id, mph.mph_node_name, mph.l1_name mph_l1_name, mph.l2_name mph_l2_name, \
                    mph.l3_name mph_l3_name, mph.l4_name mph_l4_name, mph.l5_name mph_l5_name \
                    from product.rph_flat \
                    left join product.rph_mph_xref x on rph_flat.rph_node_id = x.rph_node_id \
                    left join product.v_mph_flat mph on x.mph_node_id = mph.mph_node_id \
                        ), \
            rph_product_xref as ( \
                        select xref.* \
                        from product.rph_product_xref xref \
                        inner join product.ret_product rp on xref.ret_product_id = rp.ret_product_id \
                        qualify row_number() over(partition by xref.ret_product_id order by xref.last_seen desc) = 1 \
            ) \
            select b.banner_name,b.banner_id,rp.brand,rp.description,rp.mst_product_id,rp.ret_product_id,rp.price,rp.upc,rp.size_uofm,rp.unit_size,rp.unit_uom,mst.total_size,mst.pack_size,rp.url,rp.image_url, \
            comp_psm.l1_name comp_l1_name, \
            comp_psm.l2_name comp_l2_name, \
            comp_psm.l3_name comp_l3_name,\
            comp_psm.l4_name comp_l4_name, \
            comp_psm.l5_name comp_l5_name,rpaxf.ATTR_VALUE client_attr_value \
            from (select distinct client_name, client_id, store_set_id, COMPETITOR_BANNER_ID \
                from link.project_store_set \
                ) pss \
            inner join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
            inner join product.ret_product rp on pcss.ret_product_id = rp.ret_product_id \
            left join  comp_psm on rp.ret_product_id = comp_psm.ret_product_id \
            left join product.RET_PRODUCT_ATTRIBUTE_XREF_FLAT rpaxf on rp.RET_PRODUCT_ID = rpaxf.RET_PRODUCT_ID \
            join product.mst_product mst on rp.mst_product_id=mst.mst_product_id \
            inner join store.ret_banner b on rp.banner_id = b.banner_id \
            where client_name = 'Sam"+"\\"+"'s Club (Linking)'\
            and rpaxf.CLIENT_ID = '9215ba4e-bcb8-4ad6-9f0d-fd00a71e83fc' \
            and rp.replaced_by is null \
            and b.banner_id ="+str(comp_id)+"\
            and (nullif(trim(rp.upc), '') is not null or b.BANNER_ID = 767)"
        )



        

        if 'Sam' in client_name:
            query_exec=rhs_sams
        else:
            if self.runtype == "unlinked":
                query_exec = rhs_unlinked
            elif self.runtype == "stale":
                query_exec = rhs_stale
            else:
                print("Invalid Runtype")
        logging.info(f'Rhs query : {query_exec}')
        store_df = pd.DataFrame()
        if "store2_data.csv" in os.listdir("data/"):
            os.remove("data/store2_data.csv")
        self.cursor = self.connect_snowflake()
        self.cursor.execute(query_exec)
        download_file = "store2_data.csv"
        data = self.fetch_data(self.store_dir, download_file)
        print("RHS fetches")
        if download_file in os.listdir(self.store_dir):
            store_df = pd.read_csv(self.store_dir + download_file)
        return store_df
    
    def get_comp_data_from_file(self, client_name):
        fname = "store2-"+client_name+".csv"
        if fname in self.file_list:
            df = pd.read_csv(self.data_dir+fname)
        else:
            df = pd.DataFrame()
        return df

    def get_banner_name(self, client_name):
        query_exec = (
            "select pl.client_name,pl.banner_id,b.banner_name,count(*) \
        from link.potential_links pl \
        left join price.ret_product_client_price rpc on pl.ret_product_id = rpc.ret_product_id \
        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
        left join product.ret_product rp on pl.ret_product_id =rp.ret_product_id \
        left join store.ret_banner b on b.banner_id=pl.banner_id \
        where pl.project_active_flag \
        and pl.client_name ="
            + "'"
            + client_name
            + "'"
            + "\
        and pl.banner_id=b.banner_id \
        and (pl.link_type is null or pl.link_type in ('NOMATCH','REJECTED','INVALID')) \
        group by pl.client_name,pl.banner_id,b.banner_name"
        )

        self.cursor.execute(query_exec)
        dat = self.cursor.fetchmany(50)
        df = pd.DataFrame(
            dat, columns=["client_name", "banner_id", "banner_name", "count"]
        )
        return df

    def push_run_data(self, run_id, run_data_df,runs_table):
        if self.source_data == "DB" or self.push_to_db:
            query_exec = f"select RUN_ID from link_ml.{runs_table}"
            logging.info(f"Runs table : {runs_table}")
            if self.db == "snowflake":

                self.cursor.execute(query_exec)
                dat = self.cursor.fetchmany(100)
                df = pd.DataFrame(dat, columns=["run_id"])
                run_ids = df["run_id"].tolist()
            elif self.db == "mysql":
                df = pd.read_sql(query_exec)
                run_ids = df["RUN_ID"].tolist()
            if run_id not in run_ids:
                logging.info(f"{run_id} : RUN_ID data not in DB")
                self.push_metrics_to_db(run_data_df, runs_table)
            else:
                logging.info(f"{run_id} : RUN_ID data already in DB")
        else:
            logging.info(f'{run_id} : Source data is file')
    
    def set_runtype(self,runtype):
        self.runtype=runtype
        logging.info(f'Setting runtype to {self.runtype}')


    def get_kvi_clients(self):
        query="select c.client_id,c.name from link.VALID_CLIENTS vc inner join client.E3_CLIENT c on vc.CLIENT_ID = c.CLIENT_ID"    
        if self.db=='snowflake':    
            self.cursor.execute(query)
            dat = self.cursor.fetchmany(500)
            df = pd.DataFrame(dat, columns=['CLIENT_ID','CLIENT_NAME'])
            #df['VERTICAL']='Grocery'
            #df['PROJECT NAME']='Client Items'
            return df

    def get_all_clients(self):
        query="select * from link_ml.CLIENTS"    
        if self.db=='snowflake':    
            self.cursor.execute(query)
            dat = self.cursor.fetchmany(500)
            df = pd.DataFrame(dat, columns=['CLIENT_ID','CLIENT_NAME','VERTICAL','PROJECT NAME'])
            #df['VERTICAL']='Grocery'
            #df['PROJECT NAME']='Client Items'
            return df 
 
    def get_kvi_lhs(self,client_id):
        lhs_kvi_unlinked="select pl.product_set_member_id,pl.size_uofm,pl.total_size_uofm,pl.ret_product_id,pl.upc,pl.brand,pl.description,pl.comp_banner_name,pl.comp_banner_id,ps.shop_description,ps.l1_name,ps.l2_name,ps.l3_name,ps.l4_name,pl.project_descr, pl.pswap_member_id,pl.client_name,pl.sku,pl.image_url,pl.link_type,rp.image_url,pl.banner_id,sb.banner_name \
                        from link.potential_links pl \
                        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
                        left join product.ret_product rp on pl.ret_product_id =rp.ret_product_id \
                        left join store.ret_banner sb on sb.banner_id=pl.banner_id \
                        left join ( \
                                    select SWAP_TAG_ID \
                                    from link.PRODUCT_SWAP_TAG \
                                    where TAG_DESCRIPTION ilike '%KVI%' \
                                    and client_id = "+"'"+client_id+"'"+" \
                                    ) t on pl.TAGS like '%' || t.SWAP_TAG_ID || '%' \
                        left join (select distinct ret_product_id \
                        from product.RET_PRODUCT_ATTRIBUTE_XREF rpax \
                        inner join (select distinct ah.ATTRIBUTE_HIERARCHY_ID\
                        from product.ATTRIBUTE_HIERARCHY ah \
                        inner join product.ATTRIBUTE_HIERARCHY pah \
                        on ah.ATTRIBUTE_HIERARCHY_PARENT_ID = pah.ATTRIBUTE_HIERARCHY_ID \
                        where ah.ATTRIBUTE_HIERARCHY_LABEL ilike 'KVI%' \
                        and pah.ATTRIBUTE_HIERARCHY_LABEL = "+"'"+client_id+"'"+" \
                        group by 1) ah on rpax.ATTRIBUTE_HIERARCHY_ID = ah.ATTRIBUTE_HIERARCHY_ID \
                        where rpax.ATTRIBUTE_VALUE not in ('N', 'n', 'No', 'False') \
                        ) kvi_products on pl.RET_PRODUCT_ID = kvi_products.RET_PRODUCT_ID \
                        where pl.project_active_flag \
                        and pl.client_id = "+"'"+client_id+"'"+" \
                        and (pl.link_type is null or pl.link_type in ('NOMATCH','REJECTED','INVALID')) \
                        and lower(pl.PROJECT_DESCR) not like 'in%store%' \
                        and (pl.CLIENT_CUSTOM_ATTR:"+'"Sales Rank"'+"::string in ('0 to 500', '500 to 1000') \
                        or t.SWAP_TAG_ID is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"KVI"'+":string is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"kvi"'+"::string is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"sskvi"'+"::string is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"SSKVI"'+"::string is not null \
                        or kvi_products.RET_PRODUCT_ID is not null \
                        );"

        lhs_kvi_stale="with stale as ( \
                    select PSWAP_MEMBER_ID, COMP_BANNER_ID, \
                        max(coalesce(pcss.product_store_count, 0)) max_product_store_count, \
                        max(pcss.total_store_set_count) max_total_store_set_count, \
                        min(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0) < coalesce(st.stale_threshold, 10))  stale \
                    from link.links l \
                            inner join link.project_store_set pss \
                                        on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                            left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                                        and level = 0 \
                                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                                        and pcss.ret_product_id = l.comp_ret_product_id \
                            left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                            left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                            left join ( \
                                select cs.client_id, coalesce(max(try_parse_json(JSON_VAL):"+'"STALE-THRESHOLD"'+")::numeric, 10) stale_threshold, \
                                                coalesce(max(try_parse_json(JSON_VAL):"+'"WARNING-THRESHOLD"'+")::numeric, 40) warning_threshold \
                                from client.client_setting cs \
                                    inner join client.e3_client c on cs.client_id = c.client_id \
                                where type = 'PRODUCT_LINKING' \
                                and JSON_VAL like '%THRE%' \
                                    and c.client_id = "+"'"+client_id+"'"+" \
                                    group by cs.client_id \
                            ) st on l.client_id = st.client_id \
                        left join ( \
                                select SWAP_TAG_ID \
                                from link.PRODUCT_SWAP_TAG \
                                where TAG_DESCRIPTION ilike '%KVI%' \
                                    and client_id = "+"'"+client_id+"'"+" \
                                ) t on l.LINK_TAGS like '%' || t.SWAP_TAG_ID || '%' \
                        left join (select distinct ret_product_id \
                                    from product.RET_PRODUCT_ATTRIBUTE_XREF rpax \
                                            inner join (select distinct ah.ATTRIBUTE_HIERARCHY_ID \
                                                        from product.ATTRIBUTE_HIERARCHY ah \
                                                                inner join product.ATTRIBUTE_HIERARCHY pah \
                                                                            on ah.ATTRIBUTE_HIERARCHY_PARENT_ID = pah.ATTRIBUTE_HIERARCHY_ID \
                                                        where ah.ATTRIBUTE_HIERARCHY_LABEL ilike 'KVI%' \
                                                        and pah.ATTRIBUTE_HIERARCHY_LABEL = "+"'"+client_id+"'"+" \
                                                        group by 1) ah on rpax.ATTRIBUTE_HIERARCHY_ID = ah.ATTRIBUTE_HIERARCHY_ID \
                                where rpax.ATTRIBUTE_VALUE not in ('N', 'n', 'No', 'False') \
                                ) kvi_products on l.RET_PRODUCT_ID = kvi_products.RET_PRODUCT_ID \
                    where l.project_active_flag \
                    and l.client_id = "+"'"+client_id+"'"+" \
                    and (l.CLIENT_CUSTOM_ATTR:"+'"Sales Rank"'+"::string in ('0 to 500', '500 to 1000') \
                        or t.SWAP_TAG_ID is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"KVI"'+":string is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"kvi"'+"::string is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"sskvi"'+"::string is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"SSKVI"'+"::string is not null \
                        or kvi_products.RET_PRODUCT_ID is not null \
                        ) \
                    group by PSWAP_MEMBER_ID, COMP_BANNER_ID \
                    having stale \
                ) \
                select to_char(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0), '990.99') || ' %' as coverage, \
                l.ret_product_id,rpc.client_reg_price,rpc.sales,l.product_set_member_id,l.brand,l.description,l.upc,l.size_uofm, \
                l.client_total_size,l.client_total_uom,l.comp_banner_id,l.comp_banner_name,ps.shop_description, \
                l.comp_description,l.comp_ret_product_id,l.pswap_member_id,l.client_name,l.l1_name,l.l2_name,l.l3_name,l.sku,l.comp_sku,l.banner_id,b.banner_name \
                from link.links l \
                    inner join stale on l.PSWAP_MEMBER_ID = stale.PSWAP_MEMBER_ID and l.COMP_BANNER_ID = stale.COMP_BANNER_ID \
                    inner join link.project_store_set pss on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                    left join store.ret_banner b on b.banner_id=l.banner_id \
                    left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                        and level = 0 \
                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                        and pcss.ret_product_id = l.comp_ret_product_id \
                    left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                    left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                where l.project_active_flag \
                and l.client_id= "+"'"+client_id+"'"+" \
                and coalesce(pcss.product_store_count, 0) = 0 \
                and l.link_status ='APPROVED' \
                order by rpc.sales desc nulls last;"



        if f"store1_data.csv" in os.listdir("data/"):
            os.remove(f"data/store1_data.csv")

        if self.runtype=='unlinked':
            query_exec=lhs_kvi_unlinked
        elif self.runtype=='stale':
            query_exec=lhs_kvi_stale
        print(query_exec)
        store_df = pd.DataFrame()
        self.cursor.execute(query_exec)
        download_file = f"store1_data.csv"
        data = self.fetch_data(self.store_dir, download_file)
        print("LHS fetches")
        if download_file in os.listdir(self.store_dir):
            store_df = pd.read_csv(self.store_dir + download_file)
        return store_df


    def get_kvi_comp_list(self,client_id):
        comp_kvi_unlinked="select pl.comp_banner_id,pl.comp_banner_name, count(*) as cnt \
                        from link.potential_links pl \
                        left join product.PRODUCT_SET_MEMBER ps on pl.product_set_member_id = ps.uuid \
                        left join product.ret_product rp on pl.ret_product_id =rp.ret_product_id \
                        left join ( \
                                    select SWAP_TAG_ID \
                                    from link.PRODUCT_SWAP_TAG \
                                    where TAG_DESCRIPTION ilike '%KVI%' \
                                    and client_id = "+"'"+client_id+"'"+" \
                                    ) t on pl.TAGS like '%' || t.SWAP_TAG_ID || '%' \
                        left join (select distinct ret_product_id \
                        from product.RET_PRODUCT_ATTRIBUTE_XREF rpax \
                        inner join (select distinct ah.ATTRIBUTE_HIERARCHY_ID\
                        from product.ATTRIBUTE_HIERARCHY ah \
                        inner join product.ATTRIBUTE_HIERARCHY pah \
                        on ah.ATTRIBUTE_HIERARCHY_PARENT_ID = pah.ATTRIBUTE_HIERARCHY_ID \
                        where ah.ATTRIBUTE_HIERARCHY_LABEL ilike 'KVI%' \
                        and pah.ATTRIBUTE_HIERARCHY_LABEL = "+"'"+client_id+"'"+" \
                        group by 1) ah on rpax.ATTRIBUTE_HIERARCHY_ID = ah.ATTRIBUTE_HIERARCHY_ID \
                        where rpax.ATTRIBUTE_VALUE not in ('N', 'n', 'No', 'False') \
                        ) kvi_products on pl.RET_PRODUCT_ID = kvi_products.RET_PRODUCT_ID \
                        where pl.project_active_flag \
                        and pl.client_id = "+"'"+client_id+"'"+" \
                        and (pl.link_type is null or pl.link_type in ('NOMATCH','REJECTED','INVALID')) \
                        and lower(pl.PROJECT_DESCR) not like 'in%store%' \
                        and (pl.CLIENT_CUSTOM_ATTR:"+'"Sales Rank"'+"::string in ('0 to 500', '500 to 1000') \
                        or t.SWAP_TAG_ID is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"KVI"'+":string is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"kvi"'+"::string is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"sskvi"'+"::string is not null \
                        or pl.CLIENT_CUSTOM_ATTR:"+'"SSKVI"'+"::string is not null \
                        or kvi_products.RET_PRODUCT_ID is not null \
                        ) group by pl.comp_banner_id,pl.comp_banner_name"

        comp_kvi_stale="with stale as ( \
                    select PSWAP_MEMBER_ID, COMP_BANNER_ID, \
                        max(coalesce(pcss.product_store_count, 0)) max_product_store_count, \
                        max(pcss.total_store_set_count) max_total_store_set_count, \
                        min(coalesce((pcss.product_store_count * 100.0)/pcss.total_store_set_count, 0) < coalesce(st.stale_threshold, 10))  stale \
                    from link.links l \
                            inner join link.project_store_set pss \
                                        on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                            left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                                        and level = 0 \
                                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                                        and pcss.ret_product_id = l.comp_ret_product_id \
                            left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                            left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                            left join ( \
                                select cs.client_id, coalesce(max(try_parse_json(JSON_VAL):"+'"STALE-THRESHOLD"'+")::numeric, 10) stale_threshold, \
                                                coalesce(max(try_parse_json(JSON_VAL):"+'"WARNING-THRESHOLD"'+")::numeric, 40) warning_threshold \
                                from client.client_setting cs \
                                    inner join client.e3_client c on cs.client_id = c.client_id \
                                where type = 'PRODUCT_LINKING' \
                                and JSON_VAL like '%THRE%' \
                                    and c.client_id = "+"'"+client_id+"'"+" \
                                    group by cs.client_id \
                            ) st on l.client_id = st.client_id \
                        left join ( \
                                select SWAP_TAG_ID \
                                from link.PRODUCT_SWAP_TAG \
                                where TAG_DESCRIPTION ilike '%KVI%' \
                                    and client_id = "+"'"+client_id+"'"+" \
                                ) t on l.LINK_TAGS like '%' || t.SWAP_TAG_ID || '%' \
                        left join (select distinct ret_product_id \
                                    from product.RET_PRODUCT_ATTRIBUTE_XREF rpax \
                                            inner join (select distinct ah.ATTRIBUTE_HIERARCHY_ID \
                                                        from product.ATTRIBUTE_HIERARCHY ah \
                                                                inner join product.ATTRIBUTE_HIERARCHY pah \
                                                                            on ah.ATTRIBUTE_HIERARCHY_PARENT_ID = pah.ATTRIBUTE_HIERARCHY_ID \
                                                        where ah.ATTRIBUTE_HIERARCHY_LABEL ilike 'KVI%' \
                                                        and pah.ATTRIBUTE_HIERARCHY_LABEL = "+"'"+client_id+"'"+" \
                                                        group by 1) ah on rpax.ATTRIBUTE_HIERARCHY_ID = ah.ATTRIBUTE_HIERARCHY_ID \
                                where rpax.ATTRIBUTE_VALUE not in ('N', 'n', 'No', 'False') \
                                ) kvi_products on l.RET_PRODUCT_ID = kvi_products.RET_PRODUCT_ID \
                    where l.project_active_flag \
                    and l.client_id = "+"'"+client_id+"'"+" \
                    and (l.CLIENT_CUSTOM_ATTR:"+'"Sales Rank"'+"::string in ('0 to 500', '500 to 1000') \
                        or t.SWAP_TAG_ID is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"KVI"'+":string is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"kvi"'+"::string is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"sskvi"'+"::string is not null \
                        or l.CLIENT_CUSTOM_ATTR:"+'"SSKVI"'+"::string is not null \
                        or kvi_products.RET_PRODUCT_ID is not null \
                        ) \
                    group by PSWAP_MEMBER_ID, COMP_BANNER_ID \
                    having stale \
                ) \
                select l.comp_banner_id,l.comp_banner_name,count(*) as cnt\
                from link.links l \
                    inner join stale on l.PSWAP_MEMBER_ID = stale.PSWAP_MEMBER_ID and l.COMP_BANNER_ID = stale.COMP_BANNER_ID \
                    inner join link.project_store_set pss on l.pswap_project_id = pss.pswap_project_id and l.comp_banner_id = pss.COMPETITOR_BANNER_ID \
                    left join store.ret_banner b on b.banner_id=l.banner_id \
                    left join coverage.product_coverage_store_set pcss on pss.store_set_id = pcss.store_set_id \
                        and level = 0 \
                        and pcss.banner_id = pss.COMPETITOR_BANNER_ID \
                        and pcss.ret_product_id = l.comp_ret_product_id \
                    left join price.ret_product_client_price rpc on l.ret_product_id = rpc.ret_product_id \
                    left join product.PRODUCT_SET_MEMBER ps on l.product_set_member_id = ps.uuid \
                where l.project_active_flag \
                and l.client_id= "+"'"+client_id+"'"+" \
                and coalesce(pcss.product_store_count, 0) = 0 \
                and l.link_status ='APPROVED' \
                group by l.comp_banner_id,l.comp_banner_name;"


        if f"comp_list.csv" in os.listdir("data/"):
            os.remove(f"data/comp_list.csv")

        if self.runtype=='unlinked':
            comp_exec=comp_kvi_unlinked
        elif self.runtype=='stale':
            comp_exec=comp_kvi_stale
        self.cursor.execute(comp_exec)
        download_file=f'comp_list.csv'
        comps_df=pd.DataFrame()
        comp_ids=[]
        data=self.fetch_data(self.store_dir,download_file)
        logging.info('Comp fetches')
        if download_file in os.listdir(self.store_dir):
            comps_df = pd.read_csv(self.store_dir + download_file)

        if len(comps_df) > 1:
            comp_ids = tuple(comps_df["COMP_BANNER_ID"].tolist())
        elif len(comps_df) == 1:
            comp_ids = comps_df["COMP_BANNER_ID"].tolist()[0]
        return comp_ids, comps_df

    def create_kvi_run_id(self):
        dt1=datetime.datetime.today().date()
        week_number =dt1.isocalendar()[1]
        run_id=str(week_number)+"_WK_"+dt1.strftime('%B')+"_"+str(dt1.year)+"_KVI_RUN"
        return run_id
