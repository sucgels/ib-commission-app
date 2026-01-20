import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Data Analysis", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูล")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    cols = [c.lower() for c in df.columns]
    
    # เช็คว่ามีคอลัมน์ฝากถอนไหม
    has_net_deposit = 'deposit' in cols and 'withdraw' in cols

    if has_net_deposit:
        # --- กรณีมีข้อมูลครบ (แสดง Net Deposit) ---
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
        
        st.success("✅ ตรวจพบข้อมูลฝาก-ถอน ครบถ้วน")
        
        # แสดง Metric
        m1, m2 = st.columns(2)
        m1.metric("ยอดฝากรวม", f"{df_final['Total_Deposit'].sum():,.2f}")
        m2.metric("ยอดถอนรวม", f"{df_final['Total_Withdraw'].sum():,.2f}")

        # แสดง Treemap
        fig = px.treemap(df_final[df_final['Net_Deposit']>0], 
                         path=['Currency', 'ID'], values='Net_Deposit',
                         title="Net Deposit Treemap")
        st.plotly_chart(fig, use_container_width=True)
    else:
        # --- กรณีไม่มีข้อมูลฝากถอน (แสดงเฉพาะ Commission) ---
        st.warning("⚠️ ไฟล์นี้ไม่มีข้อมูล Deposit และ Withdraw (แสดงได้เฉพาะยอด Commission)")
        
        query = """
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Total_Commission,
            COUNT(*) AS Total_Orders
        FROM df
        GROUP BY 1, 2
        """
        df_comm = duckdb.query(query).df()
        st.dataframe(df_comm, use_container_width=True)

    # แสดงปุ่มดูข้อมูลดิบ (Raw Data) เพื่อเช็คหัวตาราง
    with st.expander("🔍 ตรวจสอบโครงสร้างไฟล์ (Raw Data)"):
        st.write("คอลัมน์ที่ตรวจพบ:", list(df.columns))
        st.dataframe(df.head(10))
