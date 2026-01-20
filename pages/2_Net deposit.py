import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Data Analysis Dashboard", layout="wide")

st.title("📊 ระบบวิเคราะห์ข้อมูลเชิงลึก")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet", type="parquet")

if uploaded_file:
    df = pd.read_parquet(uploaded_file)
    cols = [c.lower() for c in df.columns]
    has_finance = 'deposit' in cols and 'withdraw' in cols
    
    # --- ประมวลผลข้อมูลหลัก ---
    query = """
    SELECT 
        receiver_id AS ID,
        currency AS Currency,
        SUM(CAST(commission AS DOUBLE)) AS Commission,
        {finance_cols}
    FROM df
    GROUP BY 1, 2
    """.format(finance_cols="SUM(CAST(deposit AS DOUBLE)) AS Deposit, SUM(CAST(withdraw AS DOUBLE)) AS Withdraw, (SUM(CAST(deposit AS DOUBLE)) - SUM(CAST(withdraw AS DOUBLE))) AS Net_Deposit" if has_finance else "0 AS Deposit, 0 AS Withdraw, 0 AS Net_Deposit")
    
    df_final = duckdb.query(query).df()
    
    # --- ส่วนที่ 1: ตัวกรองและค้นหา (Filters) ---
    st.sidebar.header("🔍 ตัวกรองข้อมูล")
    search_id = st.sidebar.text_input("ค้นหา ID ที่ต้องการ")
    selected_currency = st.sidebar.multiselect("เลือกสกุลเงิน", options=df_final['Currency'].unique(), default=df_final['Currency'].unique())
    
    # กรองข้อมูลตามที่เลือก
    mask = df_final['Currency'].isin(selected_currency)
    if search_id:
        mask = mask & df_final['ID'].str.contains(search_id)
    df_filtered = df_final[mask].sort_values(by='Commission' if not has_finance else 'Net_Deposit', ascending=False)

    # --- ส่วนที่ 2: สรุปยอดรวม (Metrics) ---
    m1, m2, m3 = st.columns(3)
    m1.metric("จำนวน ID ทั้งหมด", f"{len(df_filtered):,}")
    m2.metric("ยอด Commission รวม", f"{df_filtered['Commission'].sum():,.2f}")
    if has_finance:
        m3.metric("ยอด Net Deposit รวม", f"{df_filtered['Net_Deposit'].sum():,.2f}")

    # --- ส่วนที่ 3: กราฟเปรียบเทียบ (Visuals) ---
    tab1, tab2 = st.tabs(["📊 กราฟแท่ง (ดูง่ายสุด)", "🌲 Treemap (ดูภาพรวม)"])
    
    with tab1:
        # แสดง Top 20 เพื่อไม่ให้กราฟแน่นเกินไป
        val_col = 'Net_Deposit' if has_finance else 'Commission'
        fig_bar = px.bar(df_filtered.head(20), x='ID', y=val_col, color='Currency',
                         text_auto='.2s', title=f"Top 20 IDs by {val_col}")
        st.plotly_chart(fig_bar, use_container_width=True)

    with tab2:
        df_tree = df_filtered[df_filtered[val_col] > 0]
        fig_tree = px.treemap(df_tree, path=['Currency', 'ID'], values=val_col, color=val_col,
                              color_continuous_scale='Blues' if not has_finance else 'RdYlGn')
        st.plotly_chart(fig_tree, use_container_width=True)

    # --- ส่วนที่ 4: ตารางละเอียด ---
    st.subheader("📋 รายละเอียดข้อมูลแบบตาราง")
    st.dataframe(df_filtered, use_container_width=True)
