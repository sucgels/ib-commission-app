import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="IB Commission Summarizer", layout="wide")

st.title("📊 ระบบสรุปยอด Commission (ฉบับเสถียร)")
st.write("อัปโหลดไฟล์ .parquet ที่คุณแปลงมาแล้วเพื่อดูผลสรุป")

# รับไฟล์ .parquet เท่านั้น
uploaded_file = st.file_uploader("เลือกไฟล์ Parquet", type="parquet")

if uploaded_file:
    # อ่านไฟล์เข้า DataFrame
    df = pd.read_parquet(uploaded_file)
    
    # ใช้ DuckDB คำนวณ (มีการ CAST ข้อมูลกลับเป็นตัวเลขเพราะเราแปลงเป็น String มา)
    query = """
    SELECT 
        receiver_id AS ID,
        ROUND(SUM(CAST(commission AS DOUBLE)), 2) AS Total_Commission,
        currency AS Currency,
        COUNT(*) AS Total_Orders
    FROM df
    GROUP BY receiver_id, currency
    ORDER BY Total_Commission DESC
    """
    
    with st.spinner("กำลังประมวลผล..."):
        df_final = duckdb.query(query).df()

    # แสดงผลสถิติเบื้องต้น
    st.success("✅ คำนวณสำเร็จ!")
    col1, col2 = st.columns(2)
    col1.metric("จำนวน ID ทั้งหมด", f"{len(df_final['ID'].unique()):,}")
    col2.metric("จำนวนรายการทั้งหมด", f"{df_final['Total_Orders'].sum():,}")

    # กราฟแสดงผล
    fig = px.bar(df_final.head(20), x='ID', y='Total_Commission', color='Currency',
                 title="Top 20 IDs by Commission", barmode='group')
    st.plotly_chart(fig, use_container_width=True)

    # ตารางข้อมูล
    st.dataframe(df_final, use_container_width=True)
