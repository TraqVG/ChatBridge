# ChatBridge

ระบบ Chat Backend ที่เชื่อมต่อ Gateway แบบ Modular

**Visibility:** Public  
**Default branch:** `main`  
**Project status:** โครงสร้างเริ่มต้นสำหรับ System Overview ยังไม่มี implementation จริง

## โมดูล

| Module | หน้าที่ |
|---|---|
| **Chat Core** | จัดการห้องแชทและข้อความ |
| **LINE OA Gateway** | เชื่อมต่อ LINE Official Account |
| **TikTok Gateway** | เชื่อมต่อ TikTok DM |
| **Message Normalization** | แปลงข้อความเป็นรูปแบบกลาง |
| **Database Adapters** | เลือก PostgreSQL / SQLite |
| **Media Storage** | จัดเก็บรูป วิดีโอ และไฟล์ |
| **Queue & Retry** | จัดคิวและจัดการงานที่ล้มเหลว |
| **Realtime** | อัปเดตข้อความทันที |
| **Chat State** | สถานะห้องแชท อ่านแล้ว เปิด/ปิด |
| **Channel Management** | จัดการการเชื่อมบัญชี LINE/TikTok |
| **Channel Identity** | จัดการตัวตนลูกค้าของแต่ละแพลตฟอร์ม |
| **Conversation Assignment** | มอบหมายแชทให้แอดมิน |
| **Audit Logs** | บันทึกกิจกรรมภายในระบบแชท |

**ขอบเขต:** ChatBridge เป็น Chat Backend ที่ตั้งใจให้ใช้ซ้ำได้กับระบบอื่น โดยไม่ผูก Business Logic ของร้าน MangMie เข้ากับ Chat Core; LINE OA Gateway และ TikTok Gateway เป็น Integration Modules ของ ChatBridge

## การตั้งชื่อและโครงสร้าง

- ใช้ชื่อโฟลเดอร์เป็นภาษาอังกฤษแบบ `kebab-case` และให้ตรงกับชื่อ Module
- แต่ละโฟลเดอร์มี `README.md` เพื่อระบุขอบเขต โดยยังไม่เลือก Framework หรือสร้างโค้ดที่ไม่ผ่าน Design Review
- Git ไม่เก็บโฟลเดอร์เปล่า จึงใช้ README ภายในทุกโฟลเดอร์
- โครงสร้างนี้เป็นข้อเสนอและสามารถเปลี่ยนชื่อได้ก่อนเริ่ม Implementation

## License

ดู [LICENSE.md](LICENSE.md) สำหรับเงื่อนไขการใช้งาน Repo นี้
