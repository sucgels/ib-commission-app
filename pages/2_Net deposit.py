import pandas as pd
import glob

# 1. ค้นหาไฟล์ Excel
file_path = "Summary_Final_Commission.xlsx"

try:
    # 2. อ่านไฟล์
    df = pd.read_excel(file_path, engine='openpyxl')

    # 3. ล้างช่องว่างที่หัวตารางทิ้งให้หมด (แก้ปัญหา 'Volume ' ไม่เท่ากับ 'Volume')
    df.columns = df.columns.astype(str).str.strip()
    
    # พิมพ์ชื่อคอลัมน์ออกมาเช็คเพื่อความชัวร์ (จะเห็นในหน้าจอตอนรัน)
    print(f"✅ ตรวจพบคอลัมน์: {list(df.columns)}")

    # 4. กำหนดชื่อคอลัมน์ (ใช้ Volume ตัว e ตามที่คุณยืนยัน)
    # หมายเหตุ: ถ้าคอลัมน์ชื่อ Referrer ของคุณสะกดเป็น Referral ให้แก้บรรทัดนี้ครับ
    vol_col = 'Volume'
    sym_col = 'Symbol'
    name_col = 'Name'
    ref_col = 'Referrer' 

    # 5. คำนวณตามเงื่อนไข
    # แปลง Volume ให้เป็นตัวเลข (ถ้ามีค่าว่างให้เป็น 0)
    df['Final_Vol'] = pd.to_numeric(df[vol_col], errors='coerce').fillna(0)
    
    # เงื่อนไข: .c หาร 100 | .s และ .p ไม่ต้องหาร
    is_c = df[sym_col].astype(str).str.strip().str.endswith('.c')
    df.loc[is_c, 'Final_Vol'] = df.loc[is_c, 'Final_Vol'] / 100

    # 6. รวมยอด (Group By)
    # ตรวจสอบว่ามีคอลัมน์ Referrer/Referral ไหม
    actual_ref_col = ref_col if ref_col in df.columns else (
        'Referral' if 'Referral' in df.columns else None
    )

    if actual_ref_col:
        summary = df.groupby([name_col, actual_ref_col], as_index=False)['Final_Vol'].sum()
    else:
        summary = df.groupby([name_col], as_index=False)['Final_Vol'].sum()

    # 7. ปัดเศษทศนิยม 4 ตำแหน่ง
    summary['Final_Vol'] = summary['Final_Vol'].round(4)

    # 8. แสดงผลและบันทึก
    print("\n--- สรุปยอด Volume เสร็จเรียบร้อย ---")
    print(summary)
    
    output_file = "Summary_Volume_Final.xlsx"
    summary.to_excel(output_file, index=False)
    print(f"\n💾 บันทึกไฟล์สำเร็จ: {output_file}")

except Exception as e:
    print(f"❌ เกิดข้อผิดพลาด: {e}")
