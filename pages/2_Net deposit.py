import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from io import BytesIO

st.set_page_config(page_title="Financial Report Dashboard", layout="wide")

st.title("📊 ระบบสรุปยอดธุรกรรมและค่าคอมมิชชัน")

uploaded_file = st.file_uploader("อัปโหลดไฟล์ Parquet (รองรับทั้งไฟล์ธุรกรรมและสรุปคอม)", type="parquet")

if uploaded_file:
    # 1. อ่านและล้างชื่อคอลัมน์
    df = pd.read_parquet(uploaded_file)
    df.columns = [str(c).strip().lower() for c in df.columns]
    cols = list(df.columns)

    # 2. ตรวจสอบประเภทไฟล์อัตโนมัติ
    if 'user id' in cols and 'amount' in cols:
        st.success("✅ ตรวจพบ: รายงานธุรกรรมรายวัน (Transaction Report)")
        query = """
        SELECT 
            "user id" AS ID,
            currency AS Currency,
            SUM(CASE WHEN lower(type) LIKE '%deposit%' THEN CAST(amount AS DOUBLE) ELSE 0 END) AS Deposit,
            SUM(CASE WHEN lower(type) LIKE '%withdraw%' THEN ABS(CAST(amount AS DOUBLE)) ELSE 0 END) AS Withdraw,
            0.0 AS Commission
        FROM df
        GROUP BY 1, 2
        """
    elif 'receiver_id' in cols:
        st.success("✅ ตรวจพบ: รายงานสรุปคอมมิชชัน (IB Commission)")
        has_f = 'deposit' in cols and 'withdraw' in cols
        query = f"""
        SELECT 
            receiver_id AS ID,
            currency AS Currency,
            SUM(CAST(commission AS DOUBLE)) AS Commission,
            {"SUM(CAST(deposit AS DOUBLE))" if has_f else "0.0"} AS Deposit,
            {"SUM(CAST(withdraw AS DOUBLE))" if has_f else "0.0"} AS Withdraw
        FROM df
        GROUP BY 1, 2
        """
    else:
        st.error("❌ ไม่รองรับรูปแบบไฟล์นี้ (ตรวจสอบคอลัมน์ ID และ Amount)")
        st.stop()

    # 3. ประมวลผลข้อมูล
    df_final = duckdb.query(query).df()
    df_final['Net_Deposit'] = df_final['Deposit'] - df_final['Withdraw']

    # 4. แสดง Metrics แยก USC / USD
    st.write("### 💰 สรุปยอดรวมแยกตามสกุลเงิน")
    summary_list = []
    
    for curr in sorted(df_final['Currency'].unique()):
        df_curr = df_final[df_final['Currency'] == curr]
        total_dep = df_curr['Deposit'].sum()
        total_wit = df_curr['Withdraw'].sum()
        total_net = df_curr['Net_Deposit'].sum()
        total_com = df_curr['Commission'].sum()
        
        # เก็บข้อมูลไว้สำหรับดาวน์โหลด
        summary_list.append({
            'Currency': curr, 'Total_ID': len(df_curr), 
            'Deposit': total_dep, 'Withdraw': total_wit, 
            'Net_Deposit': total_net, 'Commission': total_com
        })

        with st.expander(f"💵 สกุลเงิน: {curr}", expanded=True):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("จำนวน ID", f"{len(df_curr):,}")
            c2.metric("Commission", f"{total_com:,.2f}")
            c3.metric("Net Deposit", f"{total_net:,.2f}")
            c4.metric("ฝาก / ถอน", f"{total_dep:,.0f} / {total_wit:,.0f}")

    # 5. กราฟและการแสดงผล
    st.write("---")
    val_col = 'Net_Deposit' if df_final['Net_Deposit'].sum() != 0 else 'Commission'
    
    tab1, tab2, tab3 = st.tabs(["📊 Top 20 Bar Chart", "🌲 Treemap", "📝 ตารางสรุปราย ID"])
    
    with tab1:
        fig_bar = px.bar(df_final.sort_values(val_col, ascending=False).head(20), 
                         x='ID', y=val_col, color='Currency', text_auto='.2s', barmode='group')
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with tab2:
        fig_tree = px.treemap(df_final[df_final[val_col] > 0], 
                              path=['Currency', 'ID'], values=val_col, color=val_col,
                              color_continuous_scale='RdYlGn')
        st.plotly_chart(fig_tree, use_container_width=True)
        
    with tab3:
        st.dataframe(df_final.sort_values(val_col, ascending=False), use_container_width=True)

    # 6. ปุ่มดาวน์โหลดรายงานสรุป
    st.write("---")
    df_summary = pd.DataFrame(summary_list)
    
    def to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Summary')
        return output.getvalue()

    st.download_button(
        label="📥 ดาวน์โหลดรายงานสรุปแยก USC/USD (Excel)",
        data=to_excel(df_summary),
        file_name="financial_summary.xlsx",
        mime="application/vnd.ms-excel"
    )
