import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px # เพิ่ม Plotly Express เข้ามา

st.set_page_config(page_title="IB Commission Multi-Summarizer", layout="wide")

st.title("📊 ระบบสรุปยอด Commission รายบัญชี (Auto-Detect ID)")
st.write("อัปโหลดไฟล์ .csv เพื่อสรุปยอดเงินของทุกบัญชีที่ปรากฏในไฟล์")

uploaded_files = st.file_uploader("เลือกไฟล์ CSV ของคุณ", type="csv", accept_multiple_files=True)

if uploaded_files:
    if st.button("เริ่มคำนวณยอดสรุปทั้งหมด"):
        with st.spinner("กำลังประมวลผลข้อมูลมหาศาล..."):
            try:
                # รวมไฟล์ที่อัปโหลด
                all_df = [pd.read_csv(f) for f in uploaded_files]
                df_union = pd.concat(all_df)
                
                con = duckdb.connect()
                
                query = """
                SELECT 
                    receiver_id AS ID,
                    ROUND(SUM(CASE WHEN currency = 'USC' THEN commission ELSE 0 END), 2) AS Total_USC,
                    ROUND(SUM(CASE WHEN currency = 'USD' THEN commission ELSE 0 END), 2) AS Total_USD,
                    COUNT(*) AS Total_Orders
                FROM df_union
                GROUP BY receiver_id
                ORDER BY Total_USC DESC;
                """
                
                df_final = con.execute(query).df()
                
                st.success(f"✅ ประมวลผลเสร็จสิ้น! พบข้อมูลสมาชิก {len(df_final)} คน")
                
                # --- ส่วนแสดง Metrics สรุป ---
                st.subheader("ภาพรวมยอด Commission")
                col1, col2, col3 = st.columns(3)
                col1.metric("จำนวน ID ทั้งหมดที่พบ", f"{len(df_final):,.0f}")
                col2.metric("ยอดรวม USC (ทั้งหมด)", f"{df_final['Total_USC'].sum():,.2f}")
                col3.metric("ยอดรวม USD (ทั้งหมด)", f"{df_final['Total_USD'].sum():,.2f}")
                
                st.divider()

                # --- ส่วนแสดงกราฟ Top 10 ID (USC) ---
                st.subheader("ยอด Commission Top 10 ID (USC)")
                df_top10 = df_final.head(10) # เลือก 10 อันดับแรก
                
                if not df_top10.empty:
                    fig = px.bar(
                        df_top10, 
                        x='ID', 
                        y='Total_USC', 
                        title='Top 10 Receiver ID by Total USC',
                        hover_data=['Total_USD', 'Total_Orders'], # แสดงข้อมูลเพิ่มเติมเมื่อเอาเมาส์ไปชี้
                        color_discrete_sequence=px.colors.qualitative.Plotly # ใช้สีที่สวยงาม
                    )
                    fig.update_layout(xaxis_title="Receiver ID", yaxis_title="Total USC")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("ไม่พบข้อมูล Top 10 ID สำหรับแสดงกราฟ")

                st.divider()

                # --- ส่วนแสดงตารางข้อมูลทั้งหมด ---
                st.subheader("ตารางสรุปยอด Commission ทั้งหมด")
                st.dataframe(df_final, use_container_width=True)
                
                # ปุ่มดาวน์โหลด
                csv_download = df_final.to_csv(index=False).encode('utf-8-sig')
                st.download_button(
                    label="📥 ดาวน์โหลดไฟล์สรุปทุกคน (CSV)",
                    data=csv_download,
                    file_name="All_Receivers_Summary.csv",
                    mime="text/csv",
                )
                
            except Exception as e:
                st.error(f"เกิดข้อผิดพลาด: {e}")
