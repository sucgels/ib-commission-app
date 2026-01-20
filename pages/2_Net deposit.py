import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Net Deposit Analysis", layout="wide")

uploaded_file = st.file_uploader("Upload Parquet file (v5.0 only)", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    
    # ตรวจสอบว่ามีคอลัมน์ที่จำเป็นครบไหม
    required = ['deposit', 'withdraw', 'receiver_id', 'currency']
    found_cols = [c.lower() for c in df.columns]
    
    if all(col in found_cols for col in required):
        query = """
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(deposit AS DOUBLE)) AS Total_Deposit,
            SUM(CAST(withdraw AS DOUBLE)) AS Total_Withdraw,
            SUM(CAST(deposit AS DOUBLE)) - SUM(CAST(withdraw AS DOUBLE)) AS Net_Deposit
        FROM df
        GROUP BY 1, 2
        """
        df_final = duckdb.query(query).df()
        
        # แสดงผล Metric และ Treemap (ใช้โค้ดเดิมที่เคยให้ไว้ได้เลย)
        st.write("### สรุปยอดเงินฝากสุทธิ")
        st.dataframe(df_final)
    else:
        st.error(f"❌ ไฟล์นี้ไม่มีข้อมูล Deposit/Withdraw กรุณาใช้ตัวแปลง v5.0 แปลงไฟล์ใหม่ครับ")
        st.info(f"คอลัมน์ที่พบในไฟล์นี้: {list(df.columns)}")
