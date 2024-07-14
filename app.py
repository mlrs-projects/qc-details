import streamlit as st
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import psycopg2
import pandas as pd
import time
import openpyxl
from reportlab.pdfgen import canvas
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfbase import pdfmetrics
from reportlab.lib.units import inch
from reportlab.lib.pagesizes import A4,letter,landscape,A2
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage, Frame, PageTemplate
from sqlalchemy import create_engine, Table as table, Column, Integer, String, MetaData, ForeignKey,VARCHAR,Date
from sqlalchemy.types import Integer, String as SqlString
import numpy as np
from PIL import Image
from psycopg2 import sql, OperationalError, DatabaseError
from io import BytesIO
import yaml
from reportlab.lib.utils import ImageReader
import calendar
import pytz


st.set_page_config(
page_title="Quality Check",
layout="wide",
initial_sidebar_state="expanded")

class Microbiology:

    def __init__(self):
        self.product_details = pd.read_excel("Intended Use.xlsx")
        self.excel_file_path = "QC format.xlsx"
        ist = pytz.timezone('Asia/Kolkata')
        now_utc = datetime.now(pytz.utc)
        now_ist = now_utc.astimezone(ist)
        self.report_date_format = now_ist.strftime("%y%m%d")
        self.code = None
        self.details = None
        self.detail = None
        self.to_pending_lots = None
        self.Sterility = None
        self.Culture_Characteristics = None
        self.Physical_Characteristics = None
        self.lot_no = None
        self.platetype = None
        self.expiry_date = None
        self.release_date = None
        self.report_no = None
        self.generate_button = None
        self.qc_quesPC = None
        self.qc_quesCC = None
        self.ent_but = None
        self.fail_but = None
        self.update_but = None
        self.success_but = None
        self.report_incharge = None
        
    def db_connect(self,engine = False):

        
        # with open('config.yaml', 'r') as file:
        #     config = yaml.safe_load(file)
        db_config = st.secrets["Database"]


        db_user = db_config['db_user']
        db_password = db_config['db_password']
        db_host = db_config['db_host']
        db_port = db_config['db_port']
        db_name = db_config['db_name']

        connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

        if engine == True:
            conn = create_engine(connection_string)
        else:   
            conn = psycopg2.connect(db_config['conn'])

        return conn

    def streamlitcall(self):

        page_bg_color = """

        <h1 style='text-align: left; color: white; background-color: #010c48; padding: 10px 0;'>
        &nbsp; Microbiological Department
        </h1>

        <style>
        /* Increase the tab size */
        button[data-baseweb="tab"] {
            font-size: 24px;  /* Adjust the font size */
            padding: 12px 24px; /* Adjust the padding */
        }

        button[data-baseweb="tab"] div[data-testid="stMarkdownContainer"] p {
            font-size: 18px;  /* Adjust the font size of the text inside the tabs */
        }
        </style>
        <style>
        [data-testid="stAppViewContainer"]{
            background-color: #fffff;
        }
        [data-testid="stHeader"]{
            background-color: rgba(0,0,0,0)
        }
        [data-testid="stAppViewBlockContainer"]{
            padding-top: 2rem;
            position: relative;
        }
        [data-testid="stFileUploaderDropzoneInstructions"]{
            display:none;
        }
        [data-testid="stFileUploaderDropzone"]{
            background-color:rgba(0,0,0,0);
        }
        # #MainMenu {
        #     visibility: hidden;
        }
        footer {
            visibility: hidden;
        } 
        [data-testid="stDeployButton"]{
            visibility: hidden;
        }
        .st-emotion-cache-ott0ng {
            padding: 0rem;
        }
        .st-emotion-cache-fis6aj{
            padding-left: 0rem;
        }
        [data-testid="stDecoration"]{
            display: none;
        }
        [data-baseweb="tab-border"]{
            visibility: hidden;
        }
        [data-testid="stFileUploadDropzone"]{
            background-color: rgba(0,0,0,0);
        }
        .st-emotion-cache-fqsvsg{
            font-size:0.7rem;
        }
        .st-emotion-cache-1mpho7o{
            padding-left:0rem;
        }
        .st-emotion-cache-fis6aj{
            line-height:1.10rem;
        }
        .st-emotion-cache-1v7f65g .e1b2p2ww15{
            padding-top:0rem;
            padding-botton:0rem;
        }
        .st-emotion-cache-16txtl3{
            padding: 1.7rem 1.5rem;
        }
        /* Hide the viewer badge */
        .viewerBadge_container__r5tak {
            display: none !important;
        }
        
        </style>
        """
        st.markdown(page_bg_color, unsafe_allow_html=True)
        st.markdown( """
                    <style>
                    .stActionButton[data-testid="stActionButton"] {
                        display: none;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True )

        self.option  = option_menu(menu_title="",options=["Pending Approval","Make Approval","Report Generation"],orientation="horizontal")

        try:
            db_conn = self.db_connect()
            if db_conn is None:
                st.error("Database connection failed")
                return
            try:
                cur = db_conn.cursor()
                try:
                    cur.execute("SELECT lot_number FROM quality_check")
                    pending_lots_qc = cur.fetchall()
                except (OperationalError, DatabaseError) as e:
                    st.warning(f"No pending lots for quality Check")
                    return
                try:
                    cur.execute("SELECT lot_number FROM pending_lots WHERE quality_check_status = 'Processing'")
                    processing_lot = cur.fetchall()
                except (OperationalError, DatabaseError) as e:
                    st.error(f"Error fetching processing lots from pending_lots table")
                    return
                self.pending_lots_product = []
                for lot in pending_lots_qc:
                    try:
                        cur.execute("""
                            SELECT DISTINCT qc.lot_number, qc.product 
                            FROM quality_check qc
                            JOIN pending_lots pl ON qc.lot_number = pl.lot_number
                            WHERE qc.lot_number = %s AND (pl.quality_check_status = 'Processing')
                            """, (lot[0],))

                        product = cur.fetchall()

                        if product:
                            self.pending_lots_product.append(product[0][0])  # Assuming product is the second column
                        else:
                            self.pending_lots_product.append("Not found")
                    except (OperationalError, DatabaseError) as e:
                        st.error(f"Error fetching product for lot {lot[0]}: {e}")

            except (OperationalError, DatabaseError) as e:
                st.error(f"Database operation error: {e}")
            finally:
                cur.close()
                db_conn.close()

        except (OperationalError, DatabaseError) as e:
            st.error(f"Database connection error: {e}")
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")

        if self.option == "Pending Approval":

            def pending_lot_retrival_pa(pending_lot_num):
                db_con = self.db_connect()
                cur = db_con.cursor()
                cur.execute("""SELECT production_details.*, pending_lots.quality_check_status
                            FROM production_details
                            INNER JOIN pending_lots ON production_details.lot_number = pending_lots.lot_number
                                AND production_details.lot_number = pending_lots.lot_number
                            WHERE production_details.lot_number = %s """, (pending_lot_num,))
                details = pd.DataFrame(cur.fetchall(),columns =("Lot number","Product Name","Quantity","Production Date","Expiry Date","Product Image","status"))
                details = details.drop(columns=["Product Image"])

                return details

            pend_det = []
            for i in self.pending_lots_product:
                det = pending_lot_retrival_pa(i)
                if not det.empty:
                    pend_det.append(det)

            if pend_det:
                combined_df = pd.concat(pend_det, ignore_index=True)
                st.table(combined_df)
            else:
                st.warning("No data to display.")

        if self.option == "Make Approval":
            try:
                processing_lot = [item[0] for item in processing_lot]
                pending_lots_product_opt = []
                for lot in processing_lot:
                    code_to_check = int(lot[:3])
                    if code_to_check in self.product_details['Code'].values:
                        pro_name = self.product_details.loc[self.product_details['Code'] == code_to_check, 'Product Name'].values[0]
                        pro_name = pro_name.split("(")[-1]
                        pro_name = f"{lot} ({pro_name}"
                        pending_lots_product_opt.append(pro_name)

                col1,col2 = st.columns([0.5,0.5])
                with col1:
                    self.to_pending_lots = st.selectbox("Select the Lot to verify:", options=pending_lots_product_opt)
                with col2:
                    def space(n):
                        for i in range(0,n):
                            st.write(" ")
                    space(2)
                    for products, product_code in zip(self.product_details.iloc[:, 1], self.product_details.iloc[:, 2]):
                        if int(self.to_pending_lots[:3]) == product_code:
                            a = products
                            break
                    st.info(a)
                self.to_pending_lots = self.to_pending_lots.split(" ")[0]
            except:
                st.warning("No Option to Select")

    def pending_lot_retrival(self):
        try:
            db_con = self.db_connect()
            if db_con is None:
                st.error("Database connection failed")
                return None, None

            try:
                cur = db_con.cursor()
                cur.execute("""
                    SELECT production_details.*, pending_lots.quality_check_status
                    FROM production_details
                    INNER JOIN pending_lots ON production_details.lot_number = pending_lots.lot_number
                    WHERE production_details.lot_number = %s
                """, (self.to_pending_lots,))
                detail = cur.fetchall()

                columns = ["Lot number", "Product Name", "Quantity", "Production Date", "Expiry Date", "Product Image", "Status"]
                details_df = pd.DataFrame(detail, columns=columns)
                details_df = details_df.drop(columns=["Product Image"])

                cur.close()
                db_con.close()
                return details_df, detail

            except (OperationalError, DatabaseError) as e:
                st.warning("No Data Found")
                return None, None

        except (OperationalError, DatabaseError) as e:
            st.error(f"Database connection error: {e}")
            return None, None
        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            return None, None

    
    def display_pending_lots(self):
        self.details ,self.detail = self.pending_lot_retrival()

    def generate_report(self):
        try:
            if self.option == "Make Approval":

                ist = pytz.timezone('Asia/Kolkata')
                now_utc = datetime.now(pytz.utc)
                now_ist = now_utc.astimezone(ist)                
                current_time = now_ist.strftime("%H:%M %p")
                current_date = now_ist.strftime("%d-%m-%Y")

                self.display_pending_lots()

                if not self.detail or not isinstance(self.detail, list) or len(self.detail) == 0:
                    raise ValueError("No pending lots found")

                detail = self.detail[0]
                if len(detail) < 5:
                    raise ValueError("Detail list does not contain enough elements")

                self.lot_no = detail[0]
                self.platetype = detail[1].replace("\n", "")
                self.expiry_date = detail[4]

                try:
                    self.release_date = datetime.strptime(current_date, "%d-%m-%Y").date()
                except ValueError as ve:
                    raise ValueError(f"Date conversion error: {ve}")

                return self.lot_no, self.platetype, self.expiry_date, self.release_date

        except AttributeError as ae:
            st.error(f"AttributeError: {ae}")
            self.lot_no, self.platetype, self.expiry_date, self.release_date = None, None, None, None

        except ValueError as ve:
            st.error(ve)
            self.lot_no, self.platetype, self.expiry_date, self.release_date = None, None, None, None

        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")
            self.lot_no, self.platetype, self.expiry_date, self.release_date = None, None, None, None

        return self.lot_no, self.platetype, self.expiry_date, self.release_date

    def read_qcq(self,sheet_name):
        sheet_names = pd.ExcelFile(self.excel_file_path).sheet_names
        sheets_data = {}
        for sheet in sheet_names:
            self.code, tab = sheet.split("-")
            key = f"q{self.code}{tab}"
            sheets_data[key] = pd.read_excel(self.excel_file_path, sheet_name=sheet)
        
        characteristics = sheets_data[sheet_name]                    
        empty_indices = characteristics[characteristics.isnull().all(axis=1)].index
        split_dfs = []
        
        prev_index = 0
        for index in empty_indices:
            if index > prev_index:
                split_dfs.append(characteristics.iloc[prev_index:index])
            prev_index = index + 1

        if prev_index < len(characteristics):
            split_dfs.append(characteristics.iloc[prev_index:])

        return split_dfs

    def get_sterility(self,hour=int):
        col1,col2 = st.columns([0.25,0.75])
        with col1:
            result = st.selectbox(f"Sterility @ 37⁰C ± 2⁰C for {hour} h", options=["Acceptable", "Not Acceptable"])

        if result == "Acceptable":
            data = [["No Growth", "Pass"], ["Surface Colonies", "0"], ["Sub Surface Colonies", "0"], ["Swarming", "0"]]
            df = pd.DataFrame(data, columns=["Experiment", "Result"])
            with col1:
                st.dataframe(df[:1], hide_index=True, use_container_width=True)
            with col2:
                st.write(" ")
                st.dataframe(df[1:], hide_index=True, use_container_width=True)
        else:
            data = [["No Growth", "Fail"], ["Surface Colonies", None], ["Sub Surface Colonies", None], ["Swarming", None]]
            rep = pd.DataFrame(data, columns=["Experiment", "Result"])
            
            growth = rep[:1]
            with col1:
                st.dataframe(growth, hide_index=True, use_container_width=True)
            with col2:
                st.write(" ")
                up_res = st.data_editor(
                    rep[1:], 
                    use_container_width=True, 
                    column_config={
                        "Result": st.column_config.SelectboxColumn("Result", options=["0", "Less Than 5", "Greater Than 5"], required=True)
                    },
                    hide_index=True
                )
            
            df = pd.concat([growth, up_res]).reset_index(drop=True)
        return df      

    def heading(self,topic):
        st.markdown(f"""<div style="font-size:22px;"><b> {topic} </b></div> """, unsafe_allow_html=True)

    def data_collection(self):
        try:
            if self.option == "Make Approval":
                self.lot_no, self.platetype, self.expiry_date, self.release_date = self.generate_report()
                if not self.lot_no:
                    raise ValueError("Failed to generate report. Missing lot number.")

                self.code = self.lot_no.split(" ")[0][:3]
                self.qc_quesPC = f"q{self.code}PC"
                self.qc_quesCC = f"q{self.code}CC"
                self.qc_ster = f"q{self.code}sterlity"

                self.heading("Physical Characteristics")
                physical_Characteristics = self.read_qcq(self.qc_quesPC)

                for i, df in enumerate(physical_Characteristics):
                    if i > 0:
                        df.columns = df.iloc[0]
                        df = df[1:]
                    column_names = df.columns.tolist()
                    df_with_col_names = pd.DataFrame([column_names], columns=column_names)
                    df = pd.concat([df_with_col_names, df], ignore_index=True)                        
                    updated_df = st.data_editor(
                        df[1:],
                        use_container_width=True,
                        column_config={
                            "Results": st.column_config.SelectboxColumn(
                                "Results",
                                help="Result",
                                options=["Acceptable", "Not Acceptable"],
                                required=True
                            )
                        },
                        hide_index=True
                    )                        
                    updated_df = pd.concat([df_with_col_names, updated_df], ignore_index=True)
                    physical_Characteristics[i] = updated_df
                
                for df in physical_Characteristics:
                    df.columns = physical_Characteristics[0].columns
                dum1 = pd.DataFrame()
                for df in physical_Characteristics:
                    dum1 = pd.concat([dum1, df], ignore_index=True)
                self.Physical_Characteristics = dum1

                h48 = [113, 121, 122]
                code_to_check = int(self.lot_no[:3])

                no_ster = [304,305,306,307,310]
                if code_to_check not in no_ster:
                    self.heading("Sterility")
                    if code_to_check in h48:
                        self.Sterility = self.get_sterility(48)
                    else:               
                        self.Sterility = self.get_sterility(24)

                self.heading("Culture Characteristics")
                Culture_Characteristics = self.read_qcq(self.qc_quesCC)

                for i, df in enumerate(Culture_Characteristics):
                    if i > 0:
                        df.columns = df.iloc[0]
                        df = df[1:]
                    column_names = df.columns.tolist()
                    df_with_col_names = pd.DataFrame([column_names], columns=column_names)
                    df = pd.concat([df_with_col_names, df], ignore_index=True)                        
                    updated_df = st.data_editor(
                        df[1:], 
                        use_container_width=True,
                        column_config={
                            "Results": st.column_config.SelectboxColumn(
                                "Results",
                                help="Result",
                                options=["Acceptable", "Not Acceptable"],
                                required=True
                            )
                        },
                        hide_index=True
                    )                        
                    updated_df = pd.concat([df_with_col_names, updated_df], ignore_index=True)
                    Culture_Characteristics[i] = updated_df

                for df in Culture_Characteristics:
                    df.columns = Culture_Characteristics[0].columns
                dum = pd.DataFrame()
                for df in Culture_Characteristics:
                    dum = pd.concat([dum, df], ignore_index=True)
                self.Culture_Characteristics = dum
                self.ent_but = st.button("Enter")

        except AttributeError as ae:
            st.error(f"AttributeError: {ae}")

        except ValueError as ve:
            st.warning("Refresh the Page")

        except Exception as e:
            st.error(f"An unexpected error occurred: {e}")

    def save_to_db(self):
        if self.option == "Make Approval":

            self.data_collection()
            def insertpc(df, table_name):
                lot_no1,Expiry_date1,Release_date1,platetype= self.lot_no,self.expiry_date,self.release_date,self.platetype

                with self.db_connect() as db_con:
                    cur = db_con.cursor()

                    cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        sort VARCHAR(255) NOT NULL,
                        lot_number VARCHAR(255) NOT NULL,
                        Expiry_date DATE NOT NULL,
                        product VARCHAR(255) NOT NULL,
                        report_number VARCHAR(255) NOT NULL,
                        Parameter VARCHAR(255) NOT NULL,
                        Specifications VARCHAR(255) NOT NULL,
                        Results VARCHAR(255) NOT NULL,
                        FOREIGN KEY (lot_number) REFERENCES quality_check(lot_number),
                        UNIQUE (lot_number, product, report_number, Parameter)  -- Composite unique constraint
                    )
                    """)

                    try:
                        cur.execute(f"SELECT report_number FROM {table_name} WHERE lot_number = %s", (lot_no1,))
                        reportno = cur.fetchall()

                        report_numbers = [row[0] for row in reportno]
                        unique_report_numbers = np.unique(report_numbers)
                        l = len(unique_report_numbers)

                        new_report_number_index = l + 1
                        self.report_no = f"MLRS/{lot_no1[:3]}/{self.report_date_format}/{lot_no1[5:]}/{new_report_number_index}"

                        for index, row in df.iterrows():
                            cur.execute(f"""INSERT INTO {table_name} (sort,lot_number, Expiry_date, product, report_number, Parameter, Specifications, Results)
                                            VALUES (%s,%s, %s, %s, %s, %s, %s, %s)""",
                                        (row['sort'],lot_no1, Expiry_date1, self.platetype, self.report_no, row['Parameter'], row['Specification'], row['Results']))

                        cur.execute("""SELECT report_number FROM pending_lots WHERE lot_number = %s""",(lot_no1,))
                        re_no = cur.fetchall()
                        if re_no:
                            if re_no[-1][0] is None:
                                # Execute the update query
                                cur.execute("""
                                    UPDATE pending_lots 
                                    SET report_number = %s 
                                    WHERE lot_number = %s 
                                    AND report_number IS NULL;""",
                                    (self.report_no, lot_no1))
                        
                        db_con.commit()

                    except Exception as e:
                        self.report_no = f"MLRS/{lot_no1[:3]}/{self.report_date_format}/{lot_no1[5:]}/1"
                        try:
                            for index, row in df.iterrows():
                                
                                cur.execute(f"""INSERT INTO {table_name} (sort,lot_number, Expiry_date, product, report_number, Parameter, Specifications, Results)
                                                VALUES (%s,%s, %s, %s, %s, %s, %s, %s)""",
                                            (row['sort'],lot_no1, Expiry_date1, self.platetype, self.report_no, row['Parameter'], row['Specification'], row['Results']))

                            cur.execute("""SELECT report_number FROM pending_lots WHERE lot_number = %s""",(lot_no1,))
                            re_no = cur.fetchall()

                            if re_no:
                                if re_no[-1][0] is None:
                                    cur.execute("""
                                        UPDATE pending_lots 
                                        SET report_number = %s 
                                        WHERE lot_number = %s 
                                        AND report_number IS NULL;""",
                                        (self.report_no, lot_no1))
                            db_con.commit()
                        except Exception as inner_e:
                            st.error(f"An error occurred during the fallback insertion: {inner_e}")
                            db_con.rollback()
                    finally:
                        cur.close()

                return self.report_no

            def insert_dataframe(df, table_name):
                db_con= self.db_connect(engine=True)
                df.to_sql(table_name, db_con, if_exists='append',index=True)              
            
            if self.ent_but:

                if  self.Physical_Characteristics.isna().any().any() or self.Culture_Characteristics.isna().any().any():
                    st.error("Please enter All the Details")
                else:
                    try:    
                        insertpc( self.Physical_Characteristics,self.qc_quesPC)
                        Culture_Characteristics = (pd.DataFrame(self.Culture_Characteristics,columns=(pd.DataFrame(self.Culture_Characteristics).iloc[0]))).iloc[1:].reset_index(drop=True)
                        def insertdet(df):
                            df['lot_number'] = self.lot_no
                            df['expiry_date'] = self.expiry_date
                            df['product'] = self.platetype
                            df['report_number'] = self.report_no

                            columns_order = ['lot_number', 'expiry_date','product','report_number'] + [col for col in df.columns if col not in ['lot_number','expiry_date', 'product','report_number']]
                            df = df[columns_order]
                            return df
                        
                        Culture_Characteristics = insertdet(self.Culture_Characteristics)
                        a=self.lot_no[:3]

                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            sterility = pd.DataFrame(self.Sterility,columns=["Experiment","Result"])
                            sterility = insertdet(sterility)
                            insert_dataframe(sterility,self.qc_ster)
                        insert_dataframe(Culture_Characteristics,self.qc_quesCC)

                        try:
                            db_con = self.db_connect()
                            with db_con.cursor() as cur:
                                cur.execute("""
                                    UPDATE pending_lots
                                    SET quality_check_status = 'Processed'
                                    WHERE lot_number = %(lot_no)s AND report_number = %(report_no)s AND quality_check_status = 'Processing';
                                """, {'lot_no': self.lot_no, 'report_no': self.report_no})

                                db_con.commit()
                                st.success("Update Successful")
                                time.sleep(1)
                                st.experimental_rerun()
                        except Exception as e:
                            db_con.rollback()
                            st.error(f"An error Occurred: {e}")

                    except Exception as e:
                        st.error(f"An error Occurred: {e}")        
                 
    def generate_pdf(self):
        def admin_login_form():
            with st.form("Admin Login", clear_on_submit=True):
                ad_username = st.text_input("Username")
                ad_password = st.text_input("Password", type="password")
                submit = st.form_submit_button("Submit")
                if submit:
                    validate_admin_login(ad_username, ad_password)

        def validate_admin_login(username, password):
            usernames = st.secrets["usernames"]
            m1_admin_username = usernames["user4"]["name"]
            m1_admin_password = usernames["user4"]["password"]
            m2_admin_username = usernames["user5"]["name"]
            m2_admin_password = usernames["user5"]["password"]

            if username == m1_admin_username and password == m1_admin_password:
                st.session_state.permission_granted_for_madmin = True
                st.session_state.report_incharge = usernames["user4"]["incharge"]
                st.success("Login Successful")
                st.rerun()
            elif username == m2_admin_username and password == m2_admin_password:
                st.session_state.permission_granted_for_madmin = True
                st.session_state.report_incharge = usernames["user5"]["incharge"]
                st.success("Login Successful")
                st.rerun()
            elif username == "" or password == "":
                st.error("Username/Password is missing...")
            else:
                st.error("Incorrect username or password!")

        if self.option == "Report Generation":
            if "permission_granted_for_madmin" not in st.session_state:
                st.session_state.permission_granted_for_madmin = False

            if "report_incharge" not in st.session_state:
                st.session_state.report_incharge = ""

            if not st.session_state.permission_granted_for_madmin:
                admin_login_form()
            else:

                st.markdown(f"<div style='font-size:20px; font-weight:bold;'>Welcome, {st.session_state.report_incharge}!</div>", unsafe_allow_html=True)

                verify_tab,update_tab,download_tab = st.tabs(["Verify Details","Update Details","Download Reports"])
                with verify_tab:
                    try:
                        db_con = self.db_connect()
                        if db_con is None:
                            st.error("Database connection failed")

                        cur = db_con.cursor()

                        try:
                            cur.execute("""
                                SELECT lot_number
                                FROM pending_lots
                                WHERE quality_check_status = 'Processed'
                            """)
                            processed_lots = cur.fetchall()

                            processed_lot = [lot[0] for lot in processed_lots]
                            pro_lot = st.sidebar.radio("Select the Lot to Verify:", options=processed_lot)

                            for products, product_code in zip(self.product_details.iloc[:, 1], self.product_details.iloc[:, 2]):
                                if int(pro_lot[:3]) == product_code:
                                    a = products
                                    break
                            st.info(a)

                            qc_quesPC = f"q{pro_lot[:3]}PC"
                            cur.execute(f"SELECT * FROM {qc_quesPC} WHERE lot_number = '{pro_lot}'")
                            detail = cur.fetchall()
                            detail_col = cur.description

                            def get_column_names(columns):
                                column_names = [column.name for column in columns]  # Get column names from description
                                return column_names

                            column_names = get_column_names(detail_col)
                            detail_df = pd.DataFrame(detail, columns=column_names)

                            if not detail_df.empty:
                                report_no = max(detail_df["report_number"])
                            else:
                                report_no = None

                            cur.close()
                            db_con.close()
                        except (OperationalError, DatabaseError) as e:
                            st.warning("No Data Found")
                    except (OperationalError, DatabaseError) as e:
                        st.error(f"Database connection error")
                    except Exception as e:
                        st.warning("Data Not Found")

                    def data_to_report(table_name,cc = False,ster = False):
                        db_con = self.db_connect()
                        try:
                            with db_con.cursor() as cur:
                                if cc == True:
                                    query = f"""SELECT * FROM  "{table_name}" WHERE report_number = %s"""
                                else:
                                    query = f"""SELECT * FROM  {table_name} WHERE report_number = %s"""

                                cur.execute(query, (report_no,))
                                data = cur.fetchall()
                                data_column = cur.description
                                column_names = [col[0] for col in data_column]
                                df = pd.DataFrame(data, columns=column_names)

                            try:
                                df_ordered = df.sort_values(by="Tube No")
                            except KeyError:
                                try:
                                    df_ordered = df.sort_values(by="sort")
                                except KeyError:
                                        df_ordered = df.sort_values(by="Experiment")

                                if ster == True:
                                    df_ordered = df_ordered
                            return df_ordered
                        except Exception as e:
                            st.error(f"An error occurred: {e}")
                        finally:
                            cur.close()
                            db_con.close()
                    
                    try:
                        if pro_lot is not None:
                            qc_qCC = f"q{pro_lot[:3]}CC"
                            qc_ster = f"q{pro_lot[:3]}sterlity"

                            pc = data_to_report(qc_quesPC)
                            cc = data_to_report(qc_qCC,True)
                            a = pro_lot[:3]
                            no_ster = [304,305,306,307,310]
                            if int(a) not in no_ster:
                                ster = data_to_report(qc_ster)

                            def show_data(data,ster = False):
                                dat = data.drop(columns=["expiry_date","product"])
                                if ster == True:
                                    dat = dat
                                else:
                                    try:
                                        dat = dat.sort_values(by="sort")
                                        dat =dat[:-1]
                                    except KeyError:
                                        try:
                                            dat = dat.sort_values(by="sort")
                                            dat =dat[:-1]

                                        except KeyError:
                                            dat = dat.sort_values(by="Experiment")

                                    dat = dat
                                st.dataframe(dat,use_container_width=True,hide_index=True)    
                                return dat
                            self.heading("Physical Characteristics")
                            pc=show_data(pc)
                            self.heading("Cultural Characteristics")
                            cc=show_data(cc)
                            no_ster = [304,305,306,307,310]
                            if int(a) not in no_ster:
                                self.heading("Sterlity")
                                ster = show_data(ster,True)

                            col1,col2,col3 = st.columns([0.33,0.33,0.34])
                            with col1:
                                self.success_but = st.button("Quality Check Passed")
                            with col3:      
                                self.fail_but = st.button("Quality Check Failed")

                    except Exception as e:                 
                        st.error({e})
                    ist = pytz.timezone('Asia/Kolkata')
                    now_utc = datetime.now(pytz.utc)
                    now_ist = now_utc.astimezone(ist)
                    current_date = now_ist.strftime("%d-%m-%Y")
                    release_date = datetime.strptime(current_date, "%d-%m-%Y").date()

                    if self.success_but:
                        db_con = self.db_connect()
                        cur = db_con.cursor()

                        cur.execute("""
                                UPDATE pending_lots
                                SET quality_check_status = %s
                                WHERE report_number = %s ;
                            """, ('Success', report_no,))
                        
                        cur.execute("""DO $$
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1 
                                                FROM information_schema.columns 
                                                WHERE table_name='pending_lots' 
                                                AND column_name='release_date'
                                            ) THEN
                                                ALTER TABLE pending_lots 
                                                ADD COLUMN release_date DATE;
                                            END IF;
                                        END $$;
                                        UPDATE pending_lots
                                        SET release_date = %s WHERE report_number = %s ;
                                    """,(release_date,report_no,))
                        
                        cur.execute("""DO $$
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1 
                                                FROM information_schema.columns 
                                                WHERE table_name='pending_lots' 
                                                AND column_name='report_approved_by'
                                            ) THEN
                                                ALTER TABLE pending_lots 
                                                ADD COLUMN report_approved_by VARCHAR;
                                            END IF;
                                        END $$;
                                        UPDATE pending_lots
                                        SET report_approved_by = %s WHERE report_number = %s ;
                                    """,(st.session_state.report_incharge,report_no,))

                        db_con.commit()
                        db_con.close()
                        cur.close()
                        st.success("Update Successful")
                        time.sleep(1)
                        st.experimental_rerun()

                    if self.fail_but:
                        db_con = self.db_connect()
                        cur = db_con.cursor()

                        cur.execute("""
                                UPDATE pending_lots
                                SET quality_check_status = %s
                                WHERE report_number = %s ;
                            """, ('Failed', report_no,))
                        
                        cur.execute("""DO $$
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1 
                                                FROM information_schema.columns 
                                                WHERE table_name='pending_lots' 
                                                AND column_name='release_date'
                                            ) THEN
                                                ALTER TABLE pending_lots 
                                                ADD COLUMN release_date DATE;
                                            END IF;
                                        END $$;
                                        UPDATE pending_lots
                                        SET release_date = %s WHERE report_number = %s ;
                                    """,(release_date,report_no,))
                        
                        cur.execute("""DO $$
                                        BEGIN
                                            IF NOT EXISTS (
                                                SELECT 1 
                                                FROM information_schema.columns 
                                                WHERE table_name='pending_lots' 
                                                AND column_name='report_approved_by'
                                            ) THEN
                                                ALTER TABLE pending_lots 
                                                ADD COLUMN report_approved_by VARCHAR;
                                            END IF;
                                        END $$;
                                        UPDATE pending_lots
                                        SET report_approved_by = %s WHERE report_number = %s ;
                                    """,(st.session_state.report_incharge,report_no,))
                        db_con.commit()
                        db_con.close()
                        cur.close()
                        st.success("Update Successful")
                        time.sleep(1)
                        st.experimental_rerun()
                with update_tab:
                    try:
                        def update_det(df,pc=True):
                            if pc == True:
                                self.heading("Physical Characteristics")
                            else:
                                self.heading("Cultural Characteristics")

                            updated_df = st.data_editor(df,
                                use_container_width=True,
                                column_config={
                                    "Results": st.column_config.SelectboxColumn(
                                        "Results",
                                        help="Result",
                                        options=["Acceptable", "Not Acceptable"],
                                        required=True
                                    ),
                                    "results": st.column_config.SelectboxColumn(
                                        "Results",
                                        help="Result",
                                        options=["Acceptable", "Not Acceptable"],
                                        required=True
                                    )                                
                                },
                                hide_index=True
                            )                       
                            return updated_df
                        try:
                            updated_pc = update_det(pc)
                            updated_cc = update_det(cc,pc=False)
                        except:
                            st.warning("Refresh the Page")                    

                        def get_sterility(rep,hour=int):
                            col1,col2 = st.columns([0.25,0.75])
                            with col1:
                                result = st.selectbox(f"Sterility @ 37⁰C ± 2⁰C for {hour} h", options=["Acceptable", "Not Acceptable"])

                            if result == "Acceptable":
                                data = [["No Growth", "Pass"], ["Surface Colonies", "0"], ["Sub Surface Colonies", "0"], ["Swarming", "0"]]
                                df = pd.DataFrame(data, columns=["Experiment", "Result"])
                                df["lot_number"] = pro_lot
                                df["report_number"] = report_no
                                df = df[["lot_number","report_number", "Experiment", "Result"]]
                                with col1:
                                    st.dataframe(df[:1], hide_index=True, use_container_width=True)
                                with col2:
                                    st.write(" ")
                                    st.dataframe(df[1:], hide_index=True, use_container_width=True)
                            else:
                                growth = rep[:1]
                                with col1:
                                    growth.at[0, 'Result'] = "Failed"
                                    st.dataframe(growth.iloc[:,2:], hide_index=True, use_container_width=True)
                                with col2:
                                    st.write(" ")
                                    up_res = st.data_editor(
                                        rep[1:], 
                                        use_container_width=True, 
                                        column_config={
                                            "Result": st.column_config.SelectboxColumn("Result", options=["0", "Less Than 5", "Greater Than 5"], required=True)
                                        },
                                        hide_index=True
                                    )
                                
                                df = pd.concat([growth, up_res]).reset_index(drop=True)
                            return df 
                        
                        h48 = [113,121,122]
                        lot_no = pc["lot_number"][1]
                        lot_nzo = lot_no[:3]
                        no_ster = [304,305,306,307,310]
                        
                        if int(lot_nzo) not in no_ster:
                            self.heading("Sterlity")
                            code_to_check = int(lot_no[:3])
                            if code_to_check in h48:
                                ster = get_sterility(ster,48)
                            else:               
                                ster = get_sterility(ster,24)
                        self.update_but = st.button("Update Details")

                    except Exception as e:
                        st.warning(f"Data Not Found {e}")

                    col11,col12 = st.columns([0.25,0.75])
                    with col11:
                        def update_table(table_name,df,pc= False):
                            df_rows = df.to_dict(orient='records')
                            reportno =np.unique(updated_pc["report_number"])
                            lot_no = np.unique(updated_pc["lot_number"])
                            for column_values in df_rows:
                                db_con = self.db_connect()
                                cur = db_con.cursor()
                                set_clause = sql.SQL(", ").join(
                                    sql.Composed([sql.Identifier(col), sql.SQL(" = "), sql.Placeholder(col)]) 
                                    for col in column_values.keys()
                                )
                                condition_items = list(column_values.items())[:3]
                                condition_clause = sql.SQL(" AND ").join(
                                    sql.Composed([sql.Identifier(col), sql.SQL(" = "), sql.Placeholder(col)]) 
                                    for col, _ in condition_items
                                )
                                if pc==True:
                                    table_name = table_name.lower()
                                    query = sql.SQL("""
                                                        UPDATE {table}
                                                        SET {set_clause}
                                                        WHERE {condition_clause};
                                                    """).format(
                                                        table=sql.Identifier(table_name),
                                                        set_clause=set_clause,
                                                        condition_clause=condition_clause
                                                    )
                                else:
                                    query = sql.SQL("UPDATE {table} SET {values} WHERE {condition}").format(
                                        table=sql.Identifier(table_name),
                                        values=set_clause,
                                        condition=condition_clause
                                    )
                                cur.execute(query, column_values)
                                db_con.commit()
                                cur.close()
                                db_con.close()
                    if self.update_but:
                        update_table(qc_quesPC,updated_pc,pc=True)
                        update_table(qc_qCC,updated_cc)
                        lot_no = pc["lot_number"][1]
                        lot_nzo = lot_no[:3]
                        no_ster = [304,305,306,307,310]
                        if int(lot_nzo) not in no_ster:
                            update_table(qc_ster,ster)
                        st.success("Successfully Updated")
                        time.sleep(2)
                        st.rerun()
                with download_tab:
                    try:
                        release_date_in = st.date_input("Select The Report Generated Date", value="default_value_today", format="DD/MM/YYYY")
                        db_con = self.db_connect()
                        cur = db_con.cursor()
                        cur.execute("SELECT lot_number FROM pending_lots WHERE release_date = %s", (release_date_in,))
                        rep_in = cur.fetchall()
                        reles_lot = {item[0] for item in rep_in}

                        alter_but = st.checkbox("Enter Lot number")
                        if alter_but:
                            sel_reles_lot = st.text_input("Lot Number")
                        else:
                            sel_reles_lot = st.selectbox("Select the Lot", options=list(reles_lot))
                        cur.execute("SELECT report_number FROM pending_lots WHERE (lot_number = %s AND (quality_check_status = 'Success' OR quality_check_status = 'Failed' OR quality_check_status = 'Repeat'))", (sel_reles_lot,))
                        reles_rep = cur.fetchall()
                        reles_rep = {item[0] for item in reles_rep}
                        sel_reles_rep = st.radio("Select the Report Number", options=list(reles_rep))
                        pc_table_name = f"q{sel_reles_lot[:3]}pc"
                        cc_table_name = f"q{sel_reles_lot[:3]}CC"
                        ster_table_name = f"q{sel_reles_lot[:3]}sterlity"
                        lot_number = sel_reles_lot
                        report_number = sel_reles_rep
                        product_details = pd.read_excel("Intended Use.xlsx")
                    except Exception as e:
                        st.warning(f"No Data Found")
                    finally:
                        if 'cur' in locals():
                            cur.close()
                        if 'db_con' in locals():
                            db_con.close()

                    def get_column_names(columns):
                        column_names = [column[0] for column in columns]
                        return column_names

                    def data_retr(table_name, lot_number, report_number,ster = False):
                        db_con = self.db_connect()
                        try:
                            with db_con.cursor() as cur:
                                query = sql.SQL("SELECT * FROM {} WHERE lot_number = %s AND report_number = %s").format(sql.Identifier(table_name))
                                cur.execute(query, (lot_number, report_number))
                                rep_data = cur.fetchall()
                                column_names = [column[0] for column in cur.description]
                                df = pd.DataFrame(rep_data, columns=column_names)
                        finally:
                            db_con.commit()
                            db_con.close()

                        try:
                            df_ordered = df.sort_values(by="sort")
                        except KeyError:
                            try:
                                df_ordered = df.sort_values(by="sort")
                            except KeyError:
                                df_ordered = df.sort_values(by="Experiment")
                        if ster == True:
                            df_ordered = df_ordered
                        else:
                            df_ordered =df_ordered[:-1]

                        return df_ordered
                    
                    def download_report():    
                        db_con = self.db_connect()
                        cur = db_con.cursor()
                        query = sql.SQL("SELECT * FROM pending_lots WHERE lot_number = %s AND report_number = %s")
                        cur.execute(query, (lot_number, report_number))
                        rep_data = cur.fetchall()
                        column_names = [column[0] for column in cur.description]
                        pend_dat = pd.DataFrame(rep_data, columns=column_names)
                        db_con.commit()
                        cur.close()
                        db_con.close()

                        Physical_Characteristics = data_retr(pc_table_name,lot_number,report_number)
                        Culture_Characteristics = data_retr(cc_table_name,lot_number,report_number)
                        a = lot_number[:3]
                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            Sterility =  data_retr(ster_table_name,lot_number,report_number)
                        Expiry_date1,Release_date1,platetype ,rep_incharge= np.unique(Physical_Characteristics["expiry_date"])[0],pend_dat["release_date"][0],np.unique(Physical_Characteristics["product"])[0],pend_dat["report_approved_by"][0]
                        Physical_Characteristics = Physical_Characteristics.iloc[:, 5:]
                        Culture_Characteristics = Culture_Characteristics.iloc[:, 6:]
                        a = lot_number[:3]
                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            Sterility = Sterility.iloc[:, 5:]

                        def col_head(df):
                            columns = df.columns.tolist()
                            df.loc[-1] = columns
                            df = pd.concat([df.iloc[[-1]], df.iloc[:-1]]).reset_index(drop=True)
                            return df
                        Physical_Characteristics = col_head(Physical_Characteristics)
                        Culture_Characteristics = col_head(Culture_Characteristics)
                        a = lot_number[:3]
                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            Sterility = col_head(Sterility)

                        pdfmetrics.registerFont(TTFont('Arial', 'Arial.ttf'))
                        pdfmetrics.registerFont(TTFont('Arial-Bold', "arialbd.ttf"))

                        buffer = BytesIO()
                        doc = SimpleDocTemplate(buffer, pagesize=A4)
                        styles = getSampleStyleSheet()

                        arial_heading1 = ParagraphStyle(name='ArialHeading1', parent=styles['Heading1'], fontName='Arial-Bold', alignment=1)
                        arial_heading4 = ParagraphStyle(name='ArialHeading4', parent=styles['Heading3'], fontName='Arial-Bold', fontsize=24, alignment=0, spaceBefore=6, spaceAfter=3)
                        arial_body_text = ParagraphStyle(name='ArialBodyText', parent=styles['BodyText'], fontName='Arial')
                        arial_normal = ParagraphStyle(name='arial_normal', parent=styles['Normal'], fontName='Arial')

                        heading_paragraph = Paragraph("Quality Check Report", arial_heading1)
                        spacer = Spacer(1, 3, isGlue=True)

                        h48 = [113, 121, 122]
                        code_to_check = int(lot_number[:3])
                        temp = "@ 37⁰C ± 2⁰C for 48 h" if code_to_check in h48 else "@ 37⁰C ± 2⁰C for 24 h"

                        culture_condition = product_details["Culture conditions"].to_list()
                        codes = product_details["Code"].to_list()
                        intended_use = product_details["Intended Use"].to_list()

                        for cod, condition,use in zip(codes, culture_condition,intended_use):
                            if int(lot_number[:3]) == int(cod):
                                culture_condition =  condition
                                intended_use = use
                                break

                        arial_normal = styles['Normal']
                        pchp = Paragraph("Physical Characteristics:", arial_heading4)
                        sterp = Paragraph(f"Sterility {temp}:", arial_normal)
                        cchp = Paragraph("Culture Characteristics: ", arial_heading4)
                        c_con = Paragraph(f"<b>Culture Conditions</b> - {culture_condition}", arial_normal)
                        i_use = Paragraph(f"<b>Intended Use</b> - {intended_use}", arial_normal)

                        logo_path = "ALogo.png"
                        img = Image.open(logo_path)
                        img_width, img_height = img.size
                        desired_width = 2 * inch
                        desired_height = (desired_width / img_width) * img_height
                        logo = RLImage(logo_path, width=desired_width, height=desired_height, hAlign="RIGHT")

                        frame = Frame(doc.leftMargin - 0.75 * inch, doc.bottomMargin - 0.75 * inch, doc.width + 1.5 * inch, doc.height + 1.5 * inch, id='normal')
                        template = PageTemplate(id='Later', frames=[frame])
                        doc.addPageTemplates([template])

                        # Table data
                        data = [
                            ["Product", platetype],
                            ["Lot Number", lot_number],
                            ["Product Expiry Date", Expiry_date1],
                            ["Report Release Date", Release_date1],
                            ["Report No.", report_number]
                        ]

                        physical_characteristics = Physical_Characteristics.values.tolist() if isinstance(Physical_Characteristics, pd.DataFrame) else Physical_Characteristics
                        culture_characteristics = Culture_Characteristics.values.tolist()   if isinstance(Culture_Characteristics, pd.DataFrame) else Culture_Characteristics
                        
                        a = lot_number[:3]
                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            sterility = Sterility.values.tolist() if isinstance(Sterility, pd.DataFrame) else Sterility

                        def create_table(data, head=False,mac = False):
                            def calculate_column_widths(data, total_width, min_width=50):
                                col_widths = [min_width] * len(data[0])
                                for row in data:
                                    for i, cell in enumerate(row):
                                        word_length = len(str(cell))
                                        col_width = word_length * 10 + 20
                                        if i in [2, 3, 4, 5, 6]:
                                            col_width = word_length * 20 + 20
                                        if col_width > col_widths[i]:
                                            col_widths[i] = col_width
                                total_natural_width = sum(col_widths)
                                scale_factor = total_width / total_natural_width
                                col_widths = [width * scale_factor for width in col_widths]
                                return col_widths

                            total_width = doc.width + 1.25 * inch
                            col_widths = calculate_column_widths(data, total_width)

                            if head == True:
                                data_with_wrapping = [
                                    [
                                        Paragraph(str(data[row_index][col_index]), arial_heading4) if row_index == 0 or row_index == 3 else Paragraph(str(data[row_index][col_index]), arial_body_text)
                                        for col_index in range(len(data[0]))
                                    ] for row_index in range(len(data))
                                ]
                            elif mac == True:
                                data_with_wrapping = [
                                    [
                                        Paragraph(str(data[row_index][col_index]), arial_heading4) if row_index == 0 or row_index == 4 else Paragraph(str(data[row_index][col_index]), arial_body_text)
                                        for col_index in range(len(data[0]))
                                    ] for row_index in range(len(data))
                                ]

                            else:
                                data_with_wrapping = [
                                    [
                                        Paragraph(str(data[row_index][col_index]), arial_heading4) if row_index == 0 else Paragraph(str(data[row_index][col_index]), arial_body_text)
                                        for col_index in range(len(data[0]))
                                    ] for row_index in range(len(data))
                                ]


                            table = Table(data_with_wrapping, colWidths=col_widths)
                            table_style = TableStyle([
                                ('BACKGROUND', (0, 0), (-1, 0), colors.transparent),
                                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                                ('FONTNAME', (0, 0), (-1, -1), 'Arial'),
                                ('FONTSIZE', (0, 0), (-1, 0), 12),
                                ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                                ('BACKGROUND', (0, 1), (-1, -1), colors.transparent),
                                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                            ])
                            table.setStyle(table_style)
                            return table


                        # Create tables
                        input_data = create_table(data)
                        a = lot_number[:3]
                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            sterility_table = create_table(sterility)
                        mac = [307]
                        if int(a) in mac:                            
                            physical_characteristics_table = create_table(physical_characteristics,head=True)
                        else:
                            physical_characteristics_table = create_table(physical_characteristics)

                        head = [104, 113, 121]
                        mac = [307]
                        if int(a) in head:
                            culture_characteristics_table = create_table(culture_characteristics, head=True)
                        elif int(a) in mac:
                            culture_characteristics_table = create_table(culture_characteristics, mac=True)
                        else:
                            culture_characteristics_table = create_table(culture_characteristics)


                        # Storage and shelf life paragraph
                        storage_paragraph_text = "<b>Storage and Shelf Life:</b> Store at 2-8°C. Use before expiry period on the label."
                        storage_paragraph = Paragraph(storage_paragraph_text, arial_normal)

                        certification_paragraph_text = (
                            "This is to certify that a representative sample of this lot was tested by standard operating procedures, which include "
                            "the methods and control ATCC® cultures specified in Quality Assurance for Commercially Prepared Microbiological Culture "
                            "Media (CLSI Standard). The results reported were obtained at the time of release."
                        )
                        certification_paragraph = Paragraph(certification_paragraph_text, arial_body_text)
                        report_approved_by_text = f"Report Checked and Verified By <b>{rep_incharge}</b>"
                        arial_normal1 = styles["Normal"]
                        right_aligned_style = ParagraphStyle(name="RightAligned", parent=arial_normal1, alignment=2)  # alignment=2 corresponds to "right"

                        report_approved_by = Paragraph(report_approved_by_text, right_aligned_style)

                        a = lot_number[:3]
                        no_ster = [304,305,306,307,310]
                        if int(a) not in no_ster:
                            elements = [logo, heading_paragraph, input_data, pchp, physical_characteristics_table,sterp, sterility_table, cchp, culture_characteristics_table]
                        else:
                            elements = [logo, heading_paragraph, input_data, pchp, physical_characteristics_table, cchp, culture_characteristics_table]
                   
                        if culture_condition:
                            elements.append(c_con)
                        elements.append(spacer)
                        elements.append(i_use)
                        elements.append(spacer)
                        elements.append(storage_paragraph)
                        elements.append(spacer)
                        elements.append(certification_paragraph)
                        elements.append(spacer)
                        elements.append(spacer)
                        elements.append(spacer)
                        elements.append(spacer)
                        elements.append(spacer)
                        elements.append(spacer)
                        elements.append(report_approved_by)

                        doc.build(elements)
                        buffer.seek(0)
                        pdf_content= buffer.getvalue()
                        if st.button("Generate PDF"):
                            st.download_button(label="Download Report", data=pdf_content, file_name=f"{lot_number}-{report_number}.pdf", mime="application/pdf")
                    try:
                        download_report()
                    except Exception as e:
                        st.warning(f"No Data Found {e}")    
            if st.session_state.permission_granted_for_madmin == True:
                if st.sidebar.button("Admin Logout"):
                    st.session_state.permission_granted_for_madmin = False
                    st.rerun()


#####################################################################

class MLRS:

    def __init__(self):
        self.product_details = pd.DataFrame()
        self.del_detail_df = None

    def db_connect(self):
        try:

            # with open('config.yaml', 'r') as file:
            #     config = yaml.safe_load(file)
            db_config = st.secrets['Database']
            conn = psycopg2.connect(db_config['conn'])

            return conn
        
        except psycopg2.Error as e:
            st.error(f"Database connection failed: {e}")
            return None

    def streamlitcall(self):

        page_bg_color = """
        <h1 style='text-align: left; color: white; background-color: #010c48; padding: 10px 0;'>
        &nbsp; Microbiological Laboratory Research and Services
        </h1>

        <style>
        /* Increase the tab size */
        button[data-baseweb="tab"] {
            font-size: 24px;  /* Adjust the font size */
            padding: 12px 24px; /* Adjust the padding */
        }

        button[data-baseweb="tab"] div[data-testid="stMarkdownContainer"] p {
            font-size: 18px;  /* Adjust the font size of the text inside the tabs */
        }
        </style>
        <style>
        [data-testid="stAppViewContainer"]{
            background-color: #fffff;
        }
        [data-testid="stHeader"]{
            background-color: rgba(0,0,0,0)
        }
        [data-testid="stAppViewBlockContainer"]{
            padding-top: 2rem;
            position: relative;
        }
        [data-testid="stFileUploaderDropzoneInstructions"]{
            display:none;
        }
        [data-testid="stFileUploaderDropzone"]{
            background-color:rgba(0,0,0,0);
        }
        # #MainMenu {
        #     visibility: hidden;
        }
        footer {
            visibility: hidden;
        } 
        [data-testid="stDeployButton"]{
            visibility: hidden;
        }
        .st-emotion-cache-ott0ng {
            padding: 0rem;
        }
        .st-emotion-cache-fis6aj{
            padding-left: 0rem;
        }
        [data-testid="stDecoration"]{
            display: none;
        }
        [data-baseweb="tab-border"]{
            visibility: hidden;
        }
        [data-testid="stFileUploadDropzone"]{
            background-color: rgba(0,0,0,0);
        }
        .st-emotion-cache-fqsvsg{
            font-size:0.7rem;
        }
        .st-emotion-cache-1mpho7o{
            padding-left:0rem;
        }
        .st-emotion-cache-fis6aj{
            line-height:1.10rem;
        }
        .st-emotion-cache-1v7f65g .e1b2p2ww15{
            padding-top:0rem;
            padding-botton:0rem;
        }
        .st-emotion-cache-16txtl3{
            padding: 1.7rem 1.5rem;
        }
        /* Hide the viewer badge */
        .viewerBadge_container__r5tak {
            display: none !important;
        }
        
        </style>
        """
        st.markdown(page_bg_color, unsafe_allow_html=True)
    
        

        st.markdown( """
                    <style>
                    .stActionButton[data-testid="stActionButton"] {
                        display: none;
                    }
                    </style>
                    """,
                    unsafe_allow_html=True )
        

        ist = pytz.timezone('Asia/Kolkata')
        now_utc = datetime.now(pytz.utc)
        now_ist = now_utc.astimezone(ist)
        self.now = now_ist
        lot_date_format = self.now.strftime("%y%m%d")        
        date = self.now.strftime("%d /%m /%Y")
        self.option = option_menu(menu_title="", options=["Production Details", "MicroBiology Approval", "MLRS Admin"], orientation="horizontal")
        self.product_details = pd.read_excel("Intended Use.xlsx")

        if self.option == "Production Details":

            tab1, tab2, tab3 = st.tabs(["Update Details", "Quality Check Submission", "View Details"])

            with tab1:

                lot_col, num_col, dis_col = st.columns([0.5, 0.5, 0.5], gap="small")
                with lot_col:
                    product = self.product_details["Product Name"]
                    self.product = st.selectbox("Select the Product : ", options=product)
                    self.production_date = st.date_input("Production Date", format="DD/MM/YYYY")

                code = None
                for products, product_code in zip(self.product_details.iloc[:, 1], self.product_details.iloc[:, 2]):
                    if self.product == products:
                        code = product_code
                        break
                with num_col:
                    lot_number = st.number_input("Enter the Lot Number : ", min_value=1)
                    today = datetime.today()
                    two_months_from_today = today + timedelta(days=60)

                    year = today.year
                    month = today.month + 2
                    if month > 12:
                        month -= 12
                        year += 1
                    day = min(today.day, calendar.monthrange(year, month)[1])
                    two_months_from_today = datetime(year, month, day)

                    self.expiry_date = st.date_input("Select the Expiry Date :", value=two_months_from_today, format="DD/MM/YYYY")

                self.lot_number = (f"{code}{lot_date_format}{lot_number}")

                with dis_col:
                    self.quantity = st.number_input("Enter the Production Quantity : ", min_value=1, value=None, placeholder="Production Quantity")
                    self.product_image = st.file_uploader("Upload your Product image : ", type=(".jpg", ".png",".jpeg"))
                    
                    image_data = None
                    if self.product_image:
                        image_data = self.product_image.read()
 
                    st.text(f"  \n The Entire Lot Number :  {self.lot_number}")
                    st.text(" ")

                col1, col2, col3 = st.columns([0.15, 0.20, 0.65])
                with col1:
                    update_button = st.button("Update Details")
                    if update_button:
                        db_con = None
                        try:
                            db_con = self.db_connect()
                            if db_con is None:
                                raise Exception("Database connection failed")
                            cur = db_con.cursor()

                            cur.execute("""
                                CREATE TABLE IF NOT EXISTS production_details (
                                    lot_number VARCHAR(255) PRIMARY KEY,
                                    product VARCHAR(255) NOT NULL,
                                    quantity INT NOT NULL,
                                    production_date DATE NOT NULL,
                                    expiry_date DATE NOT NULL,
                                    product_image BYTEA
                                )
                            """)
                            cur.execute("""
                                CREATE TABLE IF NOT EXISTS pending_lots (
                                    lot_number VARCHAR(255) NOT NULL,
                                    quality_check_status VARCHAR(255) NOT NULL,
                                    report_number VARCHAR(255),
                                    FOREIGN KEY (lot_number) REFERENCES production_details(lot_number) ON DELETE CASCADE
                                )
                            """)
                            self.lot_status = "Pending"

                            cur.execute("""
                                INSERT INTO production_details (lot_number, product, quantity, production_date, expiry_date, product_image)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (self.lot_number, self.product, self.quantity, self.production_date, self.expiry_date, psycopg2.Binary(image_data) if self.product_image else None))

                            cur.execute("""
                                INSERT INTO pending_lots (lot_number, quality_check_status)
                                VALUES (%s, %s)
                            """, (self.lot_number, self.lot_status))
                            db_con.commit()
                            with col2:
                                st.success("Details Updated")

                        except psycopg2.IntegrityError as e:
                            error_message = str(e)

                            if "duplicate key value violates unique constraint" in error_message:
                                with col2:
                                    st.warning("Lot number already exists.")
                                    db_con.rollback()
                            elif "null value in column" in error_message:
                                with col2:
                                    st.warning("Please fill all the Details.")
                                    db_con.rollback()
                            else:
                                with col2:
                                    st.warning(f"Integrity error: {error_message}")
                        except Exception as e:
                            with col2:
                                st.error(f"An error occurred: {e}")
                                if db_con:
                                    db_con.rollback()
                        finally:
                            if db_con:
                                db_con.close()

            with tab2:
                st.sidebar.header("Pending for Quality Check Approval")
                try:
                    db_con = self.db_connect()
                    if db_con is None:
                        st.error("Database connection failed")
                        return
                    try:
                        cur = db_con.cursor()
                        try:
                            cur.execute("SELECT lot_number FROM pending_lots WHERE quality_check_status = 'Pending'")
                            self.pending_lots = cur.fetchall()
                        except (OperationalError, DatabaseError) as e:
                            st.error(f"No pending lots Found")
                            return
                        finally:
                            cur.close()

                        pending_lots_product = []
                        for lot in self.pending_lots:
                            try:
                                cur = db_con.cursor()
                                cur.execute("SELECT product FROM production_details WHERE lot_number = %s", (lot[0],))
                                product = cur.fetchone()
                                if product:
                                    pending_lots_product.append(product[0])
                                else:
                                    pending_lots_product.append("Not found")
                            except (OperationalError, DatabaseError) as e:
                                st.warning(f"Data Not Found {lot[0]}: {e}")
                            finally:
                                cur.close()

                    except (OperationalError, DatabaseError) as e:
                        st.error(f"Database operation error: {e}")
                    finally:
                        db_con.close()
                
                except (OperationalError, DatabaseError) as e:
                    st.error(f"Database connection error: {e}")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

                
                self.lot_product_options = []

                if self.pending_lots:
                    st.sidebar.write("Pending Lots:")
                    for lot, product_info in zip(self.pending_lots, pending_lots_product):
                        product_parts = product_info.split("(")
                        if len(product_parts) >= 2:
                            product = product_parts[-1].strip(" )")
                        else:
                            product = "Unknown"

                        self.lot_product_options.append(f"{lot[0]} ({product})")
                        st.sidebar.write(f"{lot[0]} ({product})")
                else:
                    st.sidebar.write("No pending lots")

                self.to_qc = st.selectbox("Select the Lots to be sent for approval", options=self.lot_product_options)
                if self.to_qc:
                    self.to_qc = self.to_qc.split(" ")[0]

                send_quality_check = st.button("Send for Quality Check")
                if send_quality_check:
                    db_con = None
                    try:
                        db_con = self.db_connect()
                        if db_con is None:
                            raise Exception("Database connection failed")
                        cur = db_con.cursor()

                        cur.execute("""
                            CREATE TABLE IF NOT EXISTS quality_check (
                                lot_number VARCHAR(255) PRIMARY KEY,
                                product VARCHAR(255) NOT NULL,
                                expiry_date DATE NOT NULL,
                                FOREIGN KEY (lot_number) REFERENCES production_details(lot_number) ON DELETE CASCADE
                            )
                        """)

                        cur.execute("""
                            SELECT product, expiry_date FROM production_details WHERE lot_number = %s
                        """, (self.to_qc,))
                        to_approve = cur.fetchall()

                        if to_approve:
                            qc_product = to_approve[0][0]
                            qc_expiry_date = to_approve[0][1]

                            cur.execute("""
                                INSERT INTO quality_check (lot_number, product, expiry_date)
                                VALUES (%s, %s, %s)
                            """, (self.to_qc, qc_product, qc_expiry_date))

                            cur.execute("""
                                UPDATE pending_lots SET quality_check_status = 'Processing' WHERE lot_number = %s;
                            """, (self.to_qc,))

                            db_con.commit()
                            st.success("Sent to quality check")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("No data found for selected lot number")

                    finally:
                        if db_con:
                            cur.close()
                            db_con.close()

            with tab3:
                db_con = self.db_connect()
                if db_con is None:
                    st.error("Database connection failed")
                    return
                cur = db_con.cursor()
                col1,col2 = st.columns([0.5,0.5])
                colu1,colu2,colu3,colu4 = st.columns([0.25,0.25,0.25,0.25])

                def view_det(lot_number):
                    db_con = self.db_connect()
                    if db_con is None:
                        st.error("Database connection failed")
                        return
                    cur = db_con.cursor()
                    try:
                        cur.execute("SELECT * FROM production_details WHERE lot_number = %s;", (lot_number,))
                        data = cur.fetchall()
                        if data:
                            df = pd.DataFrame(data, columns=("Lot number", "Product Name", "Quantity", "Production Date", "Expiry Date", "Product Image"))
                            st.table(df.drop(columns=["Product Image"]))
                            if df.iloc[0]["Product Image"]:
                                image_data = df.iloc[0]["Product Image"]
                                image = Image.open(BytesIO(image_data))
                                st.image(image, caption="Product Image",width=700)
                        else:
                            st.warning("No lot numbers found")

                    except (psycopg2.Error, IndexError) as e:
                        st.error(f"Error retrieving details: {e}")
                    cur.close()
                    db_con.close()

                if 'date_but' not in st.session_state:
                    st.session_state.date_but = False

                if 'opt_lot' not in st.session_state:
                    st.session_state.opt_lot = []

                with col1:
                    pro_date = st.date_input("Enter Production Date:", format="DD/MM/YYYY", value=None)
                with colu1:    
                    date_but = st.button("View Lot Numbers")

                if date_but:
                    st.session_state.date_but = True

                if st.session_state.date_but:
                    try:
                        cur.execute("SELECT * FROM production_details WHERE production_date = %s", (pro_date,))
                        lot_numbers = cur.fetchall()

                        if lot_numbers:
                            df = pd.DataFrame(lot_numbers, columns=("Lot number", "Product Name", "Quantity", "Production Date", "Expiry Date", "Product Image"))
                            st.table(df.drop(columns=["Product Image"]))
                            st.session_state.opt_lot = df.iloc[:, 0].to_list()
                        else:
                            st.warning("No lot numbers found for the specified production date.")
                    except psycopg2.Error as e:
                        st.error(f"Error retrieving lot numbers: {e}")

                self.view_but = False

                with colu2:
                    pro_day_det = st.checkbox("Explore Selected Day Detail")
                with col2:
                    if len(st.session_state.opt_lot) == 0:
                        lot_number = st.text_input("Enter Lot Number:", value="", placeholder="Lot Number")
                        with colu3:
                            self.view_but = st.button("View Detail")

                    else:
                        if pro_day_det:
                            lot_number = st.selectbox("Select for more Details:", options=st.session_state.opt_lot)
                            with colu3:
                                self.view_but = st.button("View Details")

                        else:
                            lot_number = st.text_input("Enter Lot Number:", value="", placeholder="Lot Number")            
                            with colu3:
                                self.view_but = st.button("View Detail")
                if self.view_but:
                    view_det(lot_number)
                with colu4:
                    ent_but = st.button("Clear")
                    if ent_but:

                        st.session_state.date_but = False
                        st.session_state.opt_but = False
                        st.rerun()

                co1,co2 = st.columns([0.5,0.5])
                with co1:
                    produc = self.product_details["Product Name"]
                    view_pro = st.selectbox("Select the product :",options=produc,)
                if st.button("View Info"):
                    cur.execute("SELECT * FROM production_details WHERE product = %s", (view_pro,))
                    lot_numbers = cur.fetchall()

                    if lot_numbers:
                        df = pd.DataFrame(lot_numbers, columns=("Lot number", "Product Name", "Quantity", "Production Date", "Expiry Date", "Product Image"))
                        st.table(df.drop(columns=["Product Image"]))
                    with co2:
                        st.text(" ")
                        st.text(" ")
                        sum1 = sum(df["Quantity"])
                        st.markdown(
                            f"""
                            <div style="font-size:20px;">
                                The Available Quantity {view_pro} : <b>{sum1}</b>
                            </div>
                            """, 
                            unsafe_allow_html=True)

                cur.close()
                db_con.close()

        if self.option == "MicroBiology Approval":

            tab1,tab2,tab3 = st.tabs(["Reports","Resending Quality Check Submission","Download Reports"])
            with tab1:
                st.sidebar.header("For More Details")

                try:
                    db_con = self.db_connect()
                    if db_con is None:
                        st.error("Database connection failed")
                        return

                    try:
                        cur = db_con.cursor()
                        try:
                            cur.execute("SELECT lot_number FROM quality_check")
                            pending_lots_qc = cur.fetchall()
                        except (OperationalError, DatabaseError) as e:
                            st.error(f"No Pending Lots")
                            return
                        finally:
                            cur.close()

                        pending_lots_product = []
                        for lot in pending_lots_qc:
                            try:
                                cur = db_con.cursor()
                                cur.execute("SELECT lot_number, product FROM quality_check WHERE lot_number = %s", (lot[0],))
                                product = cur.fetchall()
                                if product:
                                    pending_lots_product.append(product[0][0])
                                else:
                                    pending_lots_product.append("Not found")
                            except (OperationalError, DatabaseError) as e:
                                st.error(f"Error fetching product for lot {lot[0]} from quality_check: {e}")
                            finally:
                                cur.close()

                    except (OperationalError, DatabaseError) as e:
                        st.error(f"Database operation error: {e}")
                    finally:
                        db_con.close()

                except (OperationalError, DatabaseError) as e:
                    st.error(f"Database connection error: {e}")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {e}")

                pending_lots_product_opt = []
                for lot in pending_lots_product:
                    code_to_check = int(lot[:3])
                    if code_to_check in self.product_details['Code'].values:
                        pro_name = self.product_details.loc[self.product_details['Code'] == code_to_check, 'Product Name'].values[0]
                        pro_name = pro_name.split("(")[-1]
                        pro_name = f"{lot} ({pro_name}"
                        pending_lots_product_opt.append(pro_name)
                
                to_pending_lots = st.sidebar.selectbox("Select the Lot to verify:", options=pending_lots_product_opt,index=0,placeholder="Select the lot")
                to_pending_lots = to_pending_lots.split(" ")[0]
                def pending_lot_retrival(pending_lot_num):
                    db_con = self.db_connect()
                    if db_con is None:
                        return pd.DataFrame()
                    cur = db_con.cursor()
                    cur.execute("""
                        SELECT 
                            production_details.lot_number,
                            production_details.product,
                            production_details.quantity,
                            production_details.production_date,
                            production_details.expiry_date,
                            production_details.product_image,
                            pending_lots.quality_check_status,
                            pending_lots.report_number
                        FROM 
                            production_details
                        INNER JOIN 
                            pending_lots 
                        ON 
                            production_details.lot_number = pending_lots.lot_number
                        WHERE 
                            production_details.lot_number = %s
                    """, (pending_lot_num,))

                    details = cur.fetchall()
                    data_column = cur.description
                    column_names = [col[0] for col in data_column]
                    details = pd.DataFrame(details, columns=column_names)                
                    
                    cur.close()
                    db_con.close()
                    return details

                self.details = pending_lot_retrival(to_pending_lots)
                if not self.details.empty:
                    if "product_image" in self.details.columns:
                        st.dataframe(self.details.drop(columns=["product_image"]),hide_index=True,use_container_width=True)
                        if self.details.iloc[0]["product_image"]:
                            image_data = self.details.iloc[0]["product_image"]
                            image = Image.open(BytesIO(image_data))
                            st.image(image, caption="Product Image", use_column_width=True)
                    else:
                        st.dataframe(self.details,hide_index=True,use_container_width=True)

                pend_det = []
                for i in pending_lots_product:
                    det = pending_lot_retrival(i)
                    if not det.empty:
                        pend_det.append(det)

                if pend_det:

                    combined_df = pd.concat(pend_det, ignore_index=True)
                    combined_df['report_number_sort'] = pd.to_numeric(combined_df['report_number'].str.split('/').str[-1], errors='coerce')
                    none_df = combined_df[combined_df['report_number_sort'].isnull()]
                    non_none_df = combined_df.dropna(subset=['report_number_sort'])
                    result1 = non_none_df.loc[non_none_df.groupby('lot_number')['report_number_sort'].idxmax()]
                    fin_df = pd.concat([result1, none_df])
                    display_df = fin_df.drop(columns=["product_image","report_number_sort"])

                    def highlight_status(val):
                        if val == 'Success':
                            color = 'green'
                        elif val == 'Failed':
                            color = 'red'
                        elif val == 'Disregard':
                            color = 'orange'
                        elif val == 'Processed':
                            color = 'blue'
                        else:
                            color = ''
                        return f'background-color: {color}'

                    # Applying the style
                    styled_df = display_df.style.applymap(highlight_status, subset=['quality_check_status'])

                    st.dataframe(styled_df, hide_index=True, use_container_width=True)

                else:
                    st.warning("No data to display.")

            with tab2:
                st.sidebar.subheader("Failed Lots")
                db_con = self.db_connect()
                cur = db_con.cursor()
                cur.execute("""SELECT DISTINCT lot_number
                                FROM pending_lots pl
                                WHERE pl.quality_check_status = 'Failed'
                                AND pl.report_number = (
                                    SELECT MAX(pl2.report_number)
                                    FROM pending_lots pl2
                                    WHERE pl2.lot_number = pl.lot_number
                                );
                                """)
                failed_lot = cur.fetchall()
                for lot in failed_lot:
                    st.sidebar.write(lot[0])
                if failed_lot:
                    try:
                        if isinstance(failed_lot[0], (list, tuple)):
                            lot_numbers = [lot[0] for lot in failed_lot]
                            fl_lot_rs = st.selectbox("Select the lot Number", options=lot_numbers)
                            col1,col2 = st.columns([0.5,0.5])
                            with col1:
                                resend_qc = st.button("Resend for Quality checck")
                            with col2:
                                disregard = st.button("Disregard")
                            if resend_qc:
                                cur.execute("""
                                    UPDATE pending_lots
                                    SET quality_check_status = 'Repeat'
                                    WHERE lot_number = %s
                                    AND (lot_number, report_number) IN (
                                        SELECT lot_number, MAX(report_number) AS max_report_number
                                        FROM pending_lots
                                        WHERE lot_number = %s
                                        AND quality_check_status = 'Failed'
                                        GROUP BY lot_number
                                    );
                                """, (fl_lot_rs, fl_lot_rs))

                                db_con.commit()

                                cur.execute("""INSERT INTO pending_lots (lot_number, quality_check_status)
                                            VALUES (%s, 'Processing');""", (fl_lot_rs,))


                            elif disregard:

                                cur.execute("""
                                    UPDATE pending_lots
                                    SET quality_check_status = 'Disregard'
                                    WHERE lot_number = %s
                                    AND (lot_number, report_number) IN (
                                        SELECT lot_number, MAX(report_number) AS max_report_number
                                        FROM pending_lots
                                        WHERE lot_number = %s
                                        AND quality_check_status = 'Failed'
                                        GROUP BY lot_number
                                    );
                                """, (fl_lot_rs, fl_lot_rs))

                                db_con.commit()
                                st.success("Resent Succesfully")
                                time.sleep(1)
                                st.rerun()
                                
                        else:
                            st.error("Unexpected data format returned from the database.")
                    except IndexError as e:
                        st.error(f"Index error occurred: {e}")
                    except Exception as e:
                        st.error(f"An error occurred: {e}")
                else:
                    st.warning("No failed lots found.")

            with tab3:
                try:
                    release_date_in = st.date_input("Select The Report Generated Date", value=None, format="DD/MM/YYYY")
                    db_con = self.db_connect()
                    cur = db_con.cursor()
                    cur.execute("SELECT lot_number FROM pending_lots WHERE (release_date = %s AND (quality_check_status = 'Success' OR quality_check_status = 'Failed' OR quality_check_status = 'Repeat' OR quality_check_status = 'Disregard'))" , (release_date_in,))
                    rep_in = cur.fetchall()
                    reles_lot = {item[0] for item in rep_in}
                    alter_but = st.checkbox("Enter Lot number")
                    if alter_but:
                        sel_reles_lot = st.text_input("Lot Number")
                    else:
                        sel_reles_lot = st.selectbox("Select the Lot", options=list(reles_lot))
                    cur.execute("SELECT report_number FROM pending_lots WHERE (lot_number = %s AND (quality_check_status = 'Success' OR quality_check_status = 'Failed' OR quality_check_status = 'Repeat' OR quality_check_status = 'Disregard'))", (sel_reles_lot,))
                    reles_rep = cur.fetchall()
                    reles_rep = {item[0] for item in reles_rep}
                    sel_reles_rep = st.radio("Select the Report Number", options=list(reles_rep))
                    pc_table_name = f"q{sel_reles_lot[:3]}pc"
                    cc_table_name = f"q{sel_reles_lot[:3]}CC"
                    ster_table_name = f"q{sel_reles_lot[:3]}sterlity"
                    lot_number = sel_reles_lot
                    report_number = sel_reles_rep
                    product_details = pd.read_excel("Intended Use.xlsx")

                except Exception as e:
                    st.warning(f"No Data Found")
                finally:
                    if 'cur' in locals():
                        cur.close()
                    if 'db_con' in locals():
                        db_con.close()

                def get_column_names(columns):
                    column_names = [column[0] for column in columns]
                    return column_names

                def data_retr(table_name, lot_number, report_number,ster = False):
                    db_con = self.db_connect()
                    try:
                        with db_con.cursor() as cur:
                            query = sql.SQL("SELECT * FROM {} WHERE lot_number = %s AND report_number = %s").format(sql.Identifier(table_name))
                            cur.execute(query, (lot_number, report_number))
                            rep_data = cur.fetchall()
                            column_names = [column[0] for column in cur.description]
                            def to_sentence_case(s):
                                    return s[0].upper() + s[1:].lower() if s else s
                            sentence_case_column_names = [to_sentence_case(col) for col in column_names]
                            df = pd.DataFrame(rep_data, columns=sentence_case_column_names)
                    finally:
                        db_con.commit()
                        db_con.close()

                    try:
                        df_ordered = df.sort_values(by="Sort")
                    except KeyError:
                        try:
                            df_ordered = df.sort_values(by="sort")
                        except KeyError:
                            df_ordered = df.sort_values(by="Experiment")
                    if ster == True:
                        df_ordered = df_ordered
                    else:
                        df_ordered =df_ordered[:-1]

                    return df_ordered

                def download_report():    
                    db_con = self.db_connect()
                    cur = db_con.cursor()
                    query = sql.SQL("SELECT * FROM pending_lots WHERE lot_number = %s AND report_number = %s")
                    cur.execute(query, (sel_reles_lot, sel_reles_rep))
                    rep_data = cur.fetchall()
                    column_names = [column[0] for column in cur.description]
                    pend_dat = pd.DataFrame(rep_data, columns=column_names)

                    db_con.commit()
                    cur.close()
                    db_con.close()

                    Physical_Characteristics = data_retr(pc_table_name,lot_number,report_number)
                    Culture_Characteristics = data_retr(cc_table_name,lot_number,report_number)
                    a = sel_reles_lot[:3]
                    no_ster = [304,305,306,307,310]
                    if int(a) not in no_ster:

                        Sterility =  data_retr(ster_table_name,lot_number,report_number,ster=True)
                    Expiry_date1,Release_date1,platetype = np.unique(Physical_Characteristics["Expiry_date"])[0],pend_dat["release_date"][0],np.unique(Physical_Characteristics["Product"])[0]

                    Physical_Characteristics = Physical_Characteristics.iloc[:, 5:]
                    Culture_Characteristics = Culture_Characteristics.iloc[:, 6:]
            
                    if int(a) not in no_ster:
                        Sterility = Sterility.iloc[:, 5:]
                    def col_head(df):
                        columns = df.columns.tolist()

                        def to_sentence_case(s):
                            return s[0].upper() + s[1:].lower() if s else s
                        sentence_case_column_names = [to_sentence_case(col) for col in columns]
                        df.loc[-1] = columns
                        df = pd.concat([df.iloc[[-1]], df.iloc[:-1]]).reset_index(drop=True)
                        return df
                    Physical_Characteristics = col_head(Physical_Characteristics)
                    Culture_Characteristics = col_head(Culture_Characteristics)
                    a = sel_reles_lot[:3]
                    no_ster = [304,305,306,307,310]
                    if int(a) not in no_ster:
                
                        Sterility = col_head(Sterility)

                    pdfmetrics.registerFont(TTFont('Arial', 'Arial.ttf'))
                    pdfmetrics.registerFont(TTFont('Arial-Bold', "arialbd.ttf"))

                    def add_footer(canvas, doc):
                        canvas.saveState()
                        sig_path = "footer.png"
                        signature_image = Image.open(sig_path)
                        image_width, image_height = signature_image.size
                        image_aspect = image_height / float(image_width)
                        image_width = 7.75 * inch
                        image_height = image_width * image_aspect
                        x = 10
                        y = 10
                        canvas.drawImage("footer.png", x, y, width=image_width, height=image_height)
                        canvas.restoreState()
                

                    # Create a document template
                    buffer = BytesIO()
                    doc = SimpleDocTemplate(buffer, pagesize=A4)
                    styles = getSampleStyleSheet()

                    # Define custom styles
                    arial_heading1 = ParagraphStyle(name='ArialHeading1', parent=styles['Heading1'], fontName='Arial-Bold', alignment=1)
                    arial_heading4 = ParagraphStyle(name='ArialHeading4', parent=styles['Heading3'], fontName='Arial-Bold', fontsize=24, alignment=0, spaceBefore=6, spaceAfter=3)
                    arial_body_text = ParagraphStyle(name='ArialBodyText', parent=styles['BodyText'], fontName='Arial')

                    # Create header elements
                    heading_paragraph = Paragraph("Certificate of Analysis, Quality and Conformity", arial_heading1)
                    spacer = Spacer(1, 3, isGlue=True)

                    # Determine sterility condition
                    h48 = [113, 121, 122]
                    code_to_check = int(lot_number[:3])
                    # temp = "@ 37⁰C ± 2⁰C for 48 h" if code_to_check in h48 else "@ 37⁰C ± 2⁰C for 24 h"
                    temp = f"@ 37\u00B0C ± 2\u00B0C for 48 h" if code_to_check in h48 else f"@ 37\u00B0C ± 2\u00B0C for 24 h"


                    culture_condition = product_details["Culture conditions"].to_list()
                    codes = product_details["Code"].to_list()
                    intended_use = product_details["Intended Use"].to_list()

                    # Iterate through the lists and match the code
                    for cod, condition,use in zip(codes, culture_condition,intended_use):
                        if int(lot_number[:3]) == int(cod):
                            culture_condition =  condition
                            condition = condition.replace("°", "\u00B0")

                            intended_use = use
                            break

                    arial_normal = styles['Normal']
                    pchp = Paragraph("Physical Characteristics:", arial_heading4)
                    
                    sterp = Paragraph(f"Sterility {temp}:", arial_normal)
                    
                    cchp = Paragraph("Culture Characteristics: ", arial_heading4)
                    c_con = Paragraph(f"<b>Culture Conditions</b> - {culture_condition}", arial_normal)
                    i_use = Paragraph(f"<b>Intended Use</b> - {intended_use}", arial_normal)

                    # Load and configure the logo image
                    logo_path = "mlrs.png"
                    img = Image.open(logo_path)
                    img_width, img_height = img.size
                    desired_width = 7.5 * inch
                    desired_height = (desired_width / img_width) * (img_height*0.75)
                    logo = RLImage(logo_path, width=desired_width, height=desired_height, hAlign="LEFT")

                    sig_path = "sig.png"
                    signature_image = Image.open(sig_path)
                    image_width, image_height = signature_image.size
                    image_aspect = image_height / float(image_width)
                    image_width = 2 * inch
                    image_height = image_width * image_aspect
                    signature = RLImage(sig_path, width=image_width, height=image_height, hAlign="RIGHT")

                    # Create the page template
                    frame = Frame(doc.leftMargin - 0.75 * inch, doc.bottomMargin - 0.3 * inch, doc.width + 1.5 * inch, doc.height + 1.2 * inch, id='normal')
                    template = PageTemplate(id='first', frames=[frame],onPage=add_footer)
                    doc.addPageTemplates([template])

                    # Table data
                    data = [
                        ["Product", platetype],
                        ["Lot Number", lot_number],
                        ["Product Expiry Date", Expiry_date1],
                        ["Report Release Date", Release_date1],
                        ["Report No.", report_number]
                    ]

                    physical_characteristics = Physical_Characteristics.values.tolist() if isinstance(Physical_Characteristics, pd.DataFrame) else Physical_Characteristics
                    culture_characteristics = Culture_Characteristics.values.tolist()   if isinstance(Culture_Characteristics, pd.DataFrame) else Culture_Characteristics
                    
                    if int(a) not in no_ster:
                        sterility = Sterility.values.tolist() if isinstance(Sterility, pd.DataFrame) else Sterility

                    def create_table(data, head=False,mac =False):
                        def calculate_column_widths(data, total_width, min_width=50):
                            col_widths = [min_width] * len(data[0])
                            for row in data:
                                for i, cell in enumerate(row):
                                    word_length = len(str(cell))
                                    col_width = word_length * 10 + 20
                                    if i in [2, 3, 4, 5, 6]:
                                        col_width = word_length * 20 + 20
                                    if col_width > col_widths[i]:
                                        col_widths[i] = col_width
                            total_natural_width = sum(col_widths)
                            scale_factor = total_width / total_natural_width
                            col_widths = [width * scale_factor for width in col_widths]
                            return col_widths

                        total_width = doc.width + 1.25 * inch
                        col_widths = calculate_column_widths(data, total_width)

                        if head == True:
                            data_with_wrapping = [
                                [
                                    Paragraph(str(data[row_index][col_index]), arial_heading4) if row_index == 0 or row_index == 3 else Paragraph(str(data[row_index][col_index]), arial_body_text)
                                    for col_index in range(len(data[0]))
                                ] for row_index in range(len(data))
                            ]
                        elif mac == True:
                            data_with_wrapping = [
                                [
                                    Paragraph(str(data[row_index][col_index]), arial_heading4) if row_index == 0 or row_index == 4 else Paragraph(str(data[row_index][col_index]), arial_body_text)
                                    for col_index in range(len(data[0]))
                                ] for row_index in range(len(data))
                            ]
                        else:
                            data_with_wrapping = [
                                [
                                    Paragraph(str(data[row_index][col_index]), arial_heading4) if row_index == 0 else Paragraph(str(data[row_index][col_index]), arial_body_text)
                                    for col_index in range(len(data[0]))
                                ] for row_index in range(len(data))
                            ]


                        table = Table(data_with_wrapping, colWidths=col_widths)
                        table_style = TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.transparent),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                            ('FONTNAME', (0, 0), (-1, -1), 'Arial'),
                            ('FONTSIZE', (0, 0), (-1, 0), 12),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
                            ('BACKGROUND', (0, 1), (-1, -1), colors.transparent),
                            ('GRID', (0, 0), (-1, -1), 1, colors.black),
                        ])
                        table.setStyle(table_style)
                        return table


                    # Create tables
                    input_data = create_table(data)

                    if int(a) not in no_ster:
                        sterility_table = create_table(sterility)
                    mac = [307]
                    if int(a) in mac:                            
                        physical_characteristics_table = create_table(physical_characteristics,head=True)
                    else:
                        physical_characteristics_table = create_table(physical_characteristics)

                    head = [104, 113, 121]
                    mac = [307]
                    if int(a) in head:
                        culture_characteristics_table = create_table(culture_characteristics, head=True)
                    elif int(a) in mac:
                        culture_characteristics_table = create_table(culture_characteristics, mac=True)
                    else:
                        culture_characteristics_table = create_table(culture_characteristics)
                    # Storage and shelf life paragraph
                    storage_paragraph_text = "<b>Storage and Shelf Life:</b> Store at 2-8°C. Use before expiry period on the label."
                    arial_normal = styles['Normal']
                    storage_paragraph = Paragraph(storage_paragraph_text, arial_normal)

#                          "This is to certify that a representative sample of this lot was tested by standard operating procedures, which include "
#                         "the methods and control ATCC® cultures specified in Quality Assurance for Commercially Prepared Microbiological Culture "
#                         "Media (CLSI Standard). The results reported were obtained at the time of release."

                    # Certification paragraph
                    certification_paragraph_text = ("""Performance was tested by <b> Microbiological Laboratory (NABL Accredited)</b> Coimbatore. 
                                                    A representative sample from this lot was tested using standard procedures and control ATCC® cultures 
                                                    per CLSI standards. The reported results were obtained at the time of release.Any issues relating to this product should be reported to Microbiological Laboratory Research and Services (I) Pvt. Ltd.
                    within 5 working days of receipt. No changes or replacements shall be made thereafter. For technical assistance, contact MLRS at +91-8098701010.""")
                    certification_paragraph = Paragraph(certification_paragraph_text, arial_normal)
                    
                    mlrs_text = """Any issues relating to this product should be reported to Microbiological Laboratory Research and Services (I) Pvt. Ltd.
                    within 5 working days of receipt. No changes or replacements shall be made thereafter. For technical assistance, contact MLRS at +91-8098701010."""
                    
                    mlrs_para = Paragraph(mlrs_text,arial_body_text)

                    test_text = """Performance Tested by  <b> Microbiological Laboratory (NABL / ISO 15189 Accredited Laboratory)</b>
                    12-A, Cowley Brown Rd, East, R.S. Puram, Coimbatore, Tamil Nadu – 641002. """
                    test_para = Paragraph(test_text,arial_normal)

                    a = lot_number[:3]
                    no_ster = [304,305,306,307,310]
                    if int(a) not in no_ster:
                        elements = [logo, heading_paragraph, input_data, pchp, physical_characteristics_table, sterp, sterility_table, cchp, culture_characteristics_table]
                    else:
                        elements = [logo, heading_paragraph, input_data, pchp, physical_characteristics_table, cchp, culture_characteristics_table]

                    if culture_condition:
                        elements.append(c_con)
                    elements.append(spacer)
                    elements.append(i_use)
                    elements.append(spacer)
                    elements.append(storage_paragraph)
                    elements.append(spacer)
                    elements.append(certification_paragraph)
                    elements.append(spacer)
                    # elements.append(mlrs_para)
                    # elements.append(spacer)
                    # elements.append(spacer)
                    # elements.append(spacer)
                    # elements.append(test_para)
                    # elements.append(spacer)
                    # elements.append(spacer)
                    elements.append(signature)

                    doc.build(elements,onFirstPage=add_footer,onLaterPages=add_footer)
                    buffer.seek(0)
                    pdf_content= buffer.getvalue()
                    if st.button("Generate PDF"):
                        st.download_button(label="Download Report", data=pdf_content, file_name=f"{lot_number}-{report_number}.pdf", mime="application/pdf")
                try:
                    download_report()
                except Exception as e:
                    st.warning(f"Refresh The Page ")   

        if self.option == "MLRS Admin":
            usernames = st.secrets["usernames"]
            admin_username = usernames["user3"]["name"]
            admin_password = usernames["user3"]["password"]

            if "permission_granted_for_admin" not in st.session_state:
                st.session_state.permission_granted_for_admin = False

            if not st.session_state.permission_granted_for_admin:
                with st.form("Admin Login", clear_on_submit=True):
                    ad_username = st.text_input("Username")
                    ad_password = st.text_input("Password", type="password")
                    submit = st.form_submit_button("Submit")

                    if submit:
                        if ad_username == admin_username and ad_password == admin_password:
                            st.session_state.permission_granted_for_admin = True
                            st.success("Login Successful")
                            st.rerun()
                        elif ad_username == "" or ad_password == "":
                            st.error("Username/Password is missing...")
                        else:
                            st.error("Incorrect username or password!")

            if st.session_state.permission_granted_for_admin:
                tab1,tab2,tab3 = st.tabs(["Reports","Modify Reports","Modify Results"])
                with tab2:

                    try:
                        col1,col2,col3 = st.columns([0.33,0.33,0.33])
                        with col1:
                            release_date_in = st.date_input("Select The Report Generated Date", value=None, format="DD/MM/YYYY")
                        db_con = self.db_connect()
                        cur = db_con.cursor()
                        cur.execute("SELECT lot_number FROM pending_lots WHERE release_date = %s", (release_date_in,))
                        rep_in = cur.fetchall()
                        reles_lot = {item[0] for item in rep_in}
                        with col2:
                            alter_but = st.checkbox("Enter Lot number")
                            if alter_but:
                                sel_up_lot = st.text_input("Lot Number")
                            else:
                                sel_reles_lot = st.selectbox("Select the Lot", options=list(reles_lot))
                        cur.execute("SELECT report_number FROM pending_lots WHERE (lot_number = %s AND (quality_check_status = 'Success' OR quality_check_status = 'Failed' OR quality_check_status = 'Repeat'))", (sel_reles_lot,))
                        reles_rep = cur.fetchall()
                        reles_rep = {item[0] for item in reles_rep}
                        with col3:
                            sel_reles_rep = st.radio("Select the Report Number", options=list(reles_rep))
                        pc_table_name = f"q{sel_reles_lot[:3]}pc"
                        cc_table_name = f"q{sel_reles_lot[:3]}CC"
                        ster_table_name = f"q{sel_reles_lot[:3]}sterlity"
                        lot_number = sel_reles_lot
                        report_number = sel_reles_rep
                        product_details = pd.read_excel("Intended Use.xlsx")
                    except Exception as e:
                        st.warning(f"No Data Found")
                    finally:
                        if 'cur' in locals():
                            cur.close()
                        if 'db_con' in locals():
                            db_con.close()

                    def data_retr(table_name, lot_number, report_number,ster=False):
                        db_con = self.db_connect()
                        try:
                            with db_con.cursor() as cur:
                                query = sql.SQL("SELECT * FROM {} WHERE lot_number = %s AND report_number = %s").format(sql.Identifier(table_name))
                                cur.execute(query, (lot_number, report_number))
                                rep_data = cur.fetchall()
                                column_names = [column[0] for column in cur.description]
                                df = pd.DataFrame(rep_data, columns=column_names)
                        finally:
                            db_con.commit()
                            db_con.close()

                        try:
                            df_ordered = df.sort_values(by="sort")
                        except KeyError:
                            try:
                                df_ordered = df.sort_values(by="sort")
                            except KeyError:
                                df_ordered = df.sort_values(by="Experiment")
                        if ster == True:
                            df_ordered = df_ordered
                        else:
                            df_ordered =df_ordered[:-1]

                        return df_ordered
                    
                    try:

                        Physical_Characteristics = data_retr(pc_table_name,lot_number,report_number)
                        Culture_Characteristics = data_retr(cc_table_name,lot_number,report_number)
                        Sterility =  data_retr(ster_table_name,lot_number,report_number,ster=True)

                        def show_data(data,ster = False):
                            dat = data.drop(columns=["expiry_date","product"])
                            return dat
                        pc=show_data(Physical_Characteristics)
                        cc=show_data(Culture_Characteristics)
                        ster = show_data(Sterility,True)

                        def update_det(df,pc=True):
                            if pc == True:
                                st.subheader("Physical Characteristics")
                            else:
                                st.subheader("Cultural Characteristics")

                            updated_df = st.data_editor(df,
                                use_container_width=True,
                                column_config={
                                    "Results": st.column_config.SelectboxColumn(
                                        "Results",
                                        help="Result",
                                        options=["Acceptable", "Not Acceptable"],
                                        required=True
                                    ),
                                    "results": st.column_config.SelectboxColumn(
                                        "Results",
                                        help="Result",
                                        options=["Acceptable", "Not Acceptable"],
                                        required=True
                                    )                                
                                },
                                hide_index=True
                            )                       
                            return updated_df
                        try:
                            updated_pc = update_det(pc)
                            updated_cc = update_det(cc,pc=False)
                        except:
                            st.warning("Refresh the Page")

                        def get_sterility(rep,hour=int):
                            col1,col2 = st.columns([0.25,0.75])
                            with col1:
                                result = st.selectbox(f"Sterility @ 37⁰C ± 2⁰C for {hour} h", options=["Acceptable", "Not Acceptable"])

                            if result == "Acceptable":
                                data = [["No Growth", "Pass"], ["Surface Colonies", "0"], ["Sub Surface Colonies", "0"], ["Swarming", "0"]]
                                df = pd.DataFrame(data, columns=["Experiment", "Result"])
                                df["lot_number"] = lot_number
                                df["report_number"] = report_number
                                df = df[["lot_number","report_number", "Experiment", "Result"]]

                                with col1:
                                    dum_df = df[:1].copy()

                                    st.dataframe(dum_df.iloc[:,2:], hide_index=True, use_container_width=True)
                                with col2:
                                    st.write(" ")
                                    st.dataframe(df[1:], hide_index=True, use_container_width=True)
                            else:
                                growth = rep[:1]
                                with col1:
                                    growth.at[0, 'Result'] = "Failed"
                                    st.dataframe(growth.iloc[:,2:], hide_index=True, use_container_width=True)
                                with col2:
                                    st.write(" ")
                                    up_res = st.data_editor(
                                        rep[1:], 
                                        use_container_width=True, 
                                        column_config={
                                            "Result": st.column_config.SelectboxColumn("Result", options=["0", "Less Than 5", "Greater Than 5"], required=True)
                                        },
                                        hide_index=True
                                    )
                                
                                df = pd.concat([growth, up_res]).reset_index(drop=True)
                            return df   
                        h48 = [113,121,122]
                        lot_no = np.unique(pc["lot_number"])
                        lot_no = lot_no[:3]
                        st.subheader("Sterlity")
                        code_to_check = int(lot_no[:3])
                        if code_to_check in h48:
                            ster = get_sterility(ster,48)
                        else:               
                            ster = get_sterility(ster,24)

                    except:
                        st.warning("Data Not Found")

                    col11,col12 = st.columns([0.25,0.75])
                    with col11:
                        def update_table(table_name,df,pc= False):
                            df_rows = df.to_dict(orient='records')
                            reportno =sel_reles_rep
                            lot_no = sel_reles_lot
                            for column_values in df_rows:
                                db_con = self.db_connect()
                                cur = db_con.cursor()
                                set_clause = sql.SQL(", ").join(
                                    sql.Composed([sql.Identifier(col), sql.SQL(" = "), sql.Placeholder(col)]) 
                                    for col in column_values.keys()
                                )
                                condition_items = list(column_values.items())[:3]
                                condition_clause = sql.SQL(" AND ").join(
                                    sql.Composed([sql.Identifier(col), sql.SQL(" = "), sql.Placeholder(col)]) 
                                    for col, _ in condition_items
                                )

                                if pc==True:
                                    table_name = table_name.lower()
                                    query = sql.SQL("""
                                                        UPDATE {table}
                                                        SET {set_clause}
                                                        WHERE {condition_clause};
                                                    """).format(
                                                        table=sql.Identifier(table_name),
                                                        set_clause=set_clause,
                                                        condition_clause=condition_clause
                                                    )
                                else:
                                    query = sql.SQL("UPDATE {table} SET {values} WHERE {condition}").format(
                                        table=sql.Identifier(table_name),
                                        values=set_clause,
                                        condition=condition_clause
                                    )
                                cur.execute(query, column_values)
                                db_con.commit()
                                cur.close()
                                db_con.close()

                    if st.button("Update Details"):
                        update_table(pc_table_name,updated_pc,pc=True)
                        update_table(cc_table_name,updated_cc)
                        update_table(ster_table_name,ster)
                        st.success("Successfully Updated")
                        time.sleep(2)
                        st.rerun()

                with tab1:
                    try:
                        db_con = self.db_connect()
                        if db_con is None:
                            raise Exception("Failed to connect to the database.")
                        
                        cur = db_con.cursor()

                        cur.execute("""
                                    SELECT lot_number, report_number 
                                    FROM pending_lots pl
                                    WHERE report_number = (
                                        SELECT MAX(report_number) FROM pending_lots WHERE lot_number = pl.lot_number
                                    );
                                    """)
                        succed_lot = cur.fetchall()

                        result_dfs = []

                        for lot in succed_lot:
                            lot_no, report = lot
                            t_name = f"q{lot_no[:3]}pc"
                            column_id = f"{t_name}.lot_number"
                            
                            query = f"""
                                    SELECT DISTINCT
                                        pd.lot_number AS Lot_Number,
                                        pd.product,
                                        pd.quantity,
                                        pd.production_date,
                                        pd.expiry_date,
                                        {column_id} AS joined_lot_number,
                                        pl.quality_check_status,
                                        pl.report_number
                                    FROM 
                                        production_details pd
                                    JOIN 
                                        {t_name} ON pd.lot_number = {column_id}
                                    JOIN 
                                        pending_lots pl ON pd.lot_number = pl.lot_number;
                                    """
                            
                            cur.execute(query)
                            succ_lot = cur.fetchall()

                            if succ_lot:
                                colnames = [desc[0] for desc in cur.description]
                                df = pd.DataFrame(succ_lot, columns=colnames)
                                result_dfs.append(df)

                        if result_dfs:
                            final_df = pd.concat(result_dfs, ignore_index=True)
                            final_df['report_number_sort'] = pd.to_numeric(final_df['report_number'].str.split('/').str[-1], errors='coerce')
                            none_df = final_df[final_df['report_number_sort'].isnull()]
                            non_none_df = final_df.dropna(subset=['report_number_sort'])
                            result1 = non_none_df.loc[non_none_df.groupby('lot_number')['report_number_sort'].idxmax()]
                            fin_df = pd.concat([result1, none_df])
                            display_df = fin_df.drop(columns=["report_number_sort"])
                            success_df = display_df[display_df['quality_check_status'] == 'Success']
                            failed_df = display_df[display_df['quality_check_status'] == 'Failed']
                            disregard_df = display_df[display_df['quality_check_status'] == 'Disregard']



                            lot_option = st.sidebar.radio("Select the Options :",options=["Passed Lot","Failed lot","Disregard Lot"])
                            if lot_option == "Passed Lot":
                                st.subheader("Passed Lots")
                                st.dataframe(success_df, hide_index=True, use_container_width=True)
                            if lot_option == "Failed lot":
                                st.subheader("Failed Lots")
                                st.dataframe(failed_df, hide_index=True, use_container_width=True)
                            if lot_option == "Disregard Lot":
                                st.subheader("Disregard Lots")
                                st.dataframe(disregard_df, hide_index=True, use_container_width=True)


                        else:
                            final_df = pd.DataFrame()

                    except Exception as e:
                        st.error("No lots Found")

                    finally:
                        try:
                            if cur:
                                cur.close()
                            if db_con:
                                db_con.close()
                        except Exception as e:
                            st.error(f"Error while closing database resources: {str(e)}")

                with tab3:

                    def get_column_names(columns):
                        return [column[0] for column in columns] 

                    col2,col1 = st.columns([0.5,0.5])
                    if 'del_detail_df' not in st.session_state:
                        st.session_state.del_detail_df = pd.DataFrame()
                    def clear_detail_df():
                        if 'detail_df' in st.session_state:
                            st.session_state.detail_df = pd.DataFrame()



                    with col1:
                        st.header("Delete Unprocessed Lots")
                        del_lot = st.text_input("Enter the Lot ")
                        coll2,coll1 = st.columns([0.8,0.2])
                        with coll2:
                            view_but_del = st.button("View Details")
                        with coll1:
                            del_but = st.button("Delete")
                            cancel_but = st.button("Cancel")

                    if del_lot:
                        if view_but_del:
                            try:
                                db_con = self.db_connect()
                                cur = db_con.cursor()
                                query = "SELECT lot_number,product,quantity,production_date,expiry_date FROM production_details WHERE lot_number = %s"
                                cur.execute(query, (del_lot,))
                                del_det = cur.fetchall()
                                detail_col = cur.description
                                column_names = get_column_names(detail_col)
                                st.session_state.del_detail_df = pd.DataFrame(del_det, columns=column_names)
                                if not st.session_state.del_detail_df.empty:
                                    st.dataframe(st.session_state.del_detail_df, hide_index=True, use_container_width=True)
                                else:
                                    st.warning("No data found for the given lot number.")
                                try:
                                    cur.execute(f"""SELECT lot_number,quality_check_status,report_number,report_approved_by FROM pending_lots WHERE lot_number = '{del_lot}'""")
                                    del_pen_det = cur.fetchall()
                                    del_col_pen = cur.description
                                    del_col_pen = get_column_names(del_col_pen)
                                    del_col_pen = pd.DataFrame(del_pen_det,columns=del_col_pen)
                                    if not del_col_pen.empty:
                                        st.dataframe(del_col_pen, hide_index=True, use_container_width=True)

                                except Exception as e:
                                    pass
                            except Exception as e:
                                st.error(f"Error fetching details: {e}")
                            finally:
                                cur.close()
                                db_con.close()
                        
                        if not st.session_state.del_detail_df.empty:
                            if del_but:
                                try:
                                    st.info("Deleting...")
                                    db_con = self.db_connect()
                                    cur = db_con.cursor()
                                    delete_query = "DELETE FROM production_details WHERE lot_number = %s"
                                    cur.execute(delete_query, (del_lot,))
                                    db_con.commit()
                                    if cur.rowcount > 0:
                                        st.success(f"{del_lot} removed successfully")
                                    else:
                                        st.warning("No data found for the given lot number.")
                                    time.sleep(1)
                                    st.experimental_rerun()
                                except psycopg2.errors.ForeignKeyViolation:
                                    st.warning("The lot has already been processed and cannot be deleted.")
                                    clear_detail_df()
                                except Exception as e:
                                    st.error(f"An error occurred: {e}")
                                    clear_detail_df()
                                    db_con.rollback()
                                finally:
                                    cur.close()
                                    db_con.close()

                            elif cancel_but:
                                clear_detail_df()
                                st.experimental_rerun()


                    with col2:
                        try:
                            up_date_in = st.date_input("Select The Report Date", value=None, format="DD/MM/YYYY")
                            db_con = self.db_connect()
                            cur = db_con.cursor()
                            cur.execute("SELECT lot_number FROM pending_lots WHERE release_date = %s", (up_date_in,))
                            rep_in = cur.fetchall()
                            up_lot = {item[0] for item in rep_in}
                            with col2:
                                alter_but = st.checkbox("Enter Lot numbers")
                                if alter_but:
                                    sel_up_lot = st.text_input("Lot Numbers")
                                else:
                                    sel_up_lot = st.selectbox("Select the Lot for Update", options=list(up_lot))
                            cur.execute("SELECT report_number FROM pending_lots WHERE (lot_number = %s AND (quality_check_status = 'Success' OR quality_check_status = 'Failed' OR quality_check_status = 'Repeat'))", (sel_up_lot,))
                            reles_rep = cur.fetchall()
                            up_rep = {item[0] for item in reles_rep}
                            with col2:
                                sel_reles_rep = st.radio("Select the Report Number for Update", options=list(up_rep))
                                change_to = st.selectbox("Change to :",options=["Success","Failed"])


                            if st.button("Update Result"):
                                try:
                                    db_con = self.db_connect()
                                    cur = db_con.cursor()
                                    cur.execute("""
                                        UPDATE pending_lots
                                        SET quality_check_status = %s
                                        WHERE lot_number = %s
                                        AND report_number = %s
                                        AND (quality_check_status= 'Success' OR quality_check_status = 'Failed');
                                    """, (change_to, sel_up_lot, sel_reles_rep))
                                    db_con.commit()
                                    if cur.rowcount > 0:
                                        st.success(f"{sel_reles_rep} Updated")
                                    else:
                                        st.warning("No data found for the given lot number.")
                                except Exception as e:
                                    st.error(f"An error occurred: {e}")
                                    db_con.rollback()
                                finally:
                                    cur.close()
                                    db_con.close()

                        except Exception as e:
                            st.warning(f"No Data Found{e}")
                        finally:
                            if 'cur' in locals():
                                cur.close()
                            if 'db_con' in locals():
                                db_con.close()


            if st.session_state.permission_granted_for_admin == True:
                if st.sidebar.button("Admin Logout"):
                    st.session_state.permission_granted_for_admin = False
                    st.rerun()

# with open('config.yaml', 'r') as file:
#     config = yaml.safe_load(file)

if not st.session_state.get('login'):
    
    def space(n):
        for i in range(0,n):
            st.write(" ")
    col1,col2 = st.columns([0.5,0.5])
    
    with col1:
        space(10)
        st.image("image.png",caption="MICROSERVE - SCIENCE FOR SERVE",width=600)
    page_bg_color = """
        <style>
        #MainMenu {
            visibility: hidden;
        }

        [data-testid="stDeployButton"]{
            visibility: hidden;
        }
        [data-testid="stDecoration"]{
            display: none;
        }
        </style>
        """
    st.markdown(page_bg_color, unsafe_allow_html=True)

    st.markdown( """
                <style>
                .stActionButton[data-testid="stActionButton"] {
                    display: none;
                }
                </style>
                """,
                unsafe_allow_html=True )

    with col2:
        space(7)
        with st.form("Login"):
            
            hide_elements_css = """
            <style>
            /* Hide the fullscreen button */
            .st-emotion-cache-1dgsbsu[title="Exit fullscreen"] {
                display: none !important;
            }
            
            /* Hide the viewer badge */
            .viewerBadge_container__r5tak {
                display: none !important;
            }
            </style>
            """
            
            # Apply the custom CSS
            st.markdown(hide_elements_css, unsafe_allow_html=True)

        
            st.markdown(
                """
                <div align='center'>
                    <h1><span style='color: #390e9e;'>Login</span></h1>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown(
                """
                <style>
                .st-emotion-cache-1hmx6xs {
                    display: none !important;
                }
                </style>
                """,
                unsafe_allow_html=True
            )

            username = st.text_input(":skyblue[Username]")
            password = st.text_input(
                ":skyblue[Password]", type="password")
            submit = st.form_submit_button(":skyblue[Submit]")

            if submit:
                usernames = st.secrets["usernames"]
                user1_name = usernames["user1"]["name"]
                user1_password = usernames["user1"]["password"]
                user2_name = usernames["user2"]["name"]
                user2_password = usernames["user2"]["password"]


                if (username == user1_name) and (password == user1_password):
                    st.session_state.login = "mlrs"
                    st.success("Login Sucessfull")
                    st.rerun()

                elif (username == user2_name) and (password == user2_password):
                    st.session_state.login = "microlab"
                    st.success("Login Sucessfull")
                    st.rerun()


                elif (username == "") or (password == ""):
                    st.error("Username/Password is missing!")

                else:
                    st.error("Incorrect username or password!")

if st.session_state.get('login') == "microlab":

    obj = Microbiology()
    obj.streamlitcall()
    obj.pending_lot_retrival()
    obj.display_pending_lots()
    obj.save_to_db()
    obj.generate_report()
    obj.generate_pdf()

elif st.session_state.get('login') == "mlrs":
    obj = MLRS()
    obj.streamlitcall()

with st.sidebar:
    logout = st.button(label="Logout", key="logout_key")
    if logout:
        st.session_state.clear()
        st.rerun()

