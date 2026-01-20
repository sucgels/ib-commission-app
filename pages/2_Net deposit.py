import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="IB Commission & Deposit Summarizer", layout="wide")

st.title("📊 ระบบสรุปยอด Commission และ Net Deposit")

uploaded_file = st.file_uploader("เลือกไฟล์ Parquet ที่แปลงแล้ว", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    
    # ใช้ DuckDB คำนวณแยกแต่ละช่อง: Deposit, Withdraw และ Net Deposit
    query = """
    SELECT 
        receiver_id AS ID,
        currency AS Currency,
        ROUND(SUM(CAST(commission AS DOUBLE)), 2) AS Total_Commission,
        ROUND(SUM(CAST(deposit AS DOUBLE)), 2) AS Total_Deposit,
        ROUND(SUM(CAST(withdraw AS DOUBLE)), 2) AS Total_Withdraw,
        ROUND(SUM(CAST(deposit AS DOUBLE)) - SUM(CAST(withdraw AS DOUBLE)), 2) AS Net_Deposit,
        COUNT(*) AS Total_Orders
    FROM df
    GROUP BY receiver_id, currency
    ORDER BY Net_Deposit DESC
    """
    
    with st.spinner("กำลังคำนวณข้อมูล..."):
        df_final = duckdb.query(query).df()

    # --- ส่วนที่ 1: สรุปภาพรวม (Metric) ---
    st.write("---")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("ยอดฝากรวม", f"{df_final['Total_Deposit'].sum():,.2f}")
    m2.metric("ยอดถอนรวม", f"{df_final['Total_Withdraw'].sum():,.2f}")
    m3.metric("ยอดฝากสุทธิ (Net)", f"{df_final['Net_Deposit'].sum():,.2f}")
    m4.metric("Commission รวม", f"{df_final['Total_Commission'].sum():,.2f}")

    # --- ส่วนที่ 2: กราฟ Treemap ---
    st.subheader("🌲 ภาพรวม Net Deposit แยกตามชั้น (Currency > ID)")
    # กรองเฉพาะค่าที่เป็นบวกสำหรับ Treemap
    df_tree = df_final[df_final['Net_Deposit'] > 0]
    fig = px.treemap(
        df_tree, 
        path=[px.Constant("All Currencies"), 'Currency', 'ID'], 
        values='Net_Deposit',
        color='Net_Deposit',
        color_continuous_scale='RdYlGn',
        hover_data=['Total_Deposit', 'Total_Withdraw']
    )
    st.plotly_chart(fig, use_container_width=True)

    # --- ส่วนที่ 3: ตารางข้อมูลละเอียด ---
    st.subheader("📋 ตารางข้อมูลสรุปแยกตาม ID")
    # แสดงตารางพร้อมจัดรูปแบบตัวเลขให้ดูง่าย
    st.dataframe(
        df_final.style.format({
            'Total_Commission': '{:,.2f}',
            'Total_Deposit': '{:,.2f}',
            'Total_Withdraw': '{:,.2f}',
            'Net_Deposit': '{:,.2f}',
            'Total_Orders': '{:,}'
        }), 
        use_container_width=True
    )