WITH
-- CTE 1: Lấy danh sách bệnh nhân ĐÃ CÓ lịch hẹn tái khám
tb_temp_co_hen AS 
(SELECT
    r.roomname AS phong_kham,
    TO_CHAR(p.patientrecorddate, 'YYYY-MM-DD') AS ngay_kham,
    p.patientrecordid AS ma_ho_so,
    p.patientcode AS ma_benh_nhan,
    p.patientname AS ten_benh_nhan,
    p.birthdayyear AS nam_sinh,
    CASE 
        WHEN p.duyetketoan_date :: date = '0001-01-01' 
            AND p.medicalrecorddate_out :: date = '0001-01-01' 
            AND p.danopdutienkhamdichvu = 1 
            THEN p.timeupdatechiphi :: date
        WHEN p.duyetketoan_date :: date = '0001-01-01' 
            AND p.danopdutienkhamdichvu = 1 
            THEN p.medicalrecorddate_out :: date
        WHEN p.danopdutienkhamdichvu !=1 THEN null 
        ELSE p.duyetketoan_date :: date
    END AS NgayRaVien,
    CASE 
        WHEN p.dm_gioitinhid = 1 THEN 'Nam' 
        WHEN p.dm_gioitinhid = 2 THEN 'Nữ' 
        ELSE '' 
    END AS gioi_tinh,
    p.patientphone AS so_dien_thoai,
    CONCAT_WS('-',
        COALESCE((SELECT dm_xaname FROM tb_dm_xa WHERE dm_xaid = p.dm_xacode LIMIT 1), ''),
        COALESCE((SELECT dm_huyenname FROM tb_dm_huyen WHERE dm_huyenid = p.dm_huyencode LIMIT 1), ''),
        COALESCE((SELECT dm_tinhname FROM tb_dm_tinh WHERE dm_tinhid = p.dm_tinhcode LIMIT 1), '')
    ) AS dia_chi,
    p.patientphone AS sdt,
    nk.nguoigioithieuname AS nguon_khach,
    (SELECT dm_hoahongnguoigioithieu_nguoigioithieuname 
     FROM tb_dm_hoahongnguoigioithieu_nguoigioithieu 
     WHERE dm_hoahongnguoigioithieu_nguoigioithieuid = 
           (SELECT dm_hoahongnguoigioithieu_nguoigioithieuid 
            FROM tb_nguoigioithieu 
            WHERE nguoigioithieuid = p.nguoigioithieuid LIMIT 1)
     LIMIT 1) AS nhom_nguon_khach, 
    nv_phu1.nhanvienname AS bac_si_kham,
    (SELECT nhanvienname 
     FROM tb_nhanvien 
     WHERE nhanvienid = (SELECT nguoinhap 
                         FROM tb_medicalrecord_henkham
                         WHERE medicalrecordid = p.medicalrecordid_kb LIMIT 1) 
     LIMIT 1) AS ten_bac_si_hen_tai_kham,
    p.chandoan_out_main_icd10 || '-' || p.chandoan_out_main AS ket_luan,
    p.chandoan_out_ex_icd10 || '-' || p.chandoan_out_ex AS ket_qua,
    TO_CHAR(m.ngayhen, 'YYYY-MM-DD') AS ngay_hen_tai_kham,
    TO_CHAR(m.ngaynhap, 'YYYY-MM-DD') AS ngay_tao,
    (SELECT loidanbacsi FROM tb_medicalrecord WHERE m.medicalrecordid = medicalrecordid LIMIT 1) AS noi_dung_hen_tai_kham,
    null AS ma_phan_nhom,
    null AS ten_phan_nhom,
    m.nguoinhap AS ma_bac_si_hen_tai_kham,
    md.tomtatketquacls AS ket_qua_can_lam_sang,
    (SELECT STRING_AGG(DISTINCT CONCAT(md.medicinename, ': ', sd.thuoc_soluong, ' viên ', sd.thuoc_huongdansudung, ' (', sd.thuoc_duongdung, ')'), '; ')
     FROM tb_servicedata sd
     LEFT JOIN tb_medicinedata md ON sd.serviceid = md.medicineid
     WHERE sd.patientrecordid = p.patientrecordid 
     AND sd.serviceid_thuocmaster IS NOT NULL
     AND md.medicinename IS NOT NULL
     AND sd.thuoc_soluong IS NOT NULL
     AND sd.thuoc_soluong > 0
     AND sd.thuoc_huongdansudung IS NOT NULL
     AND sd.thuoc_duongdung IS NOT NULL
     LIMIT 1) AS don_thuoc,
    (SELECT lydodenkham 
     FROM tb_medicalrecord_khambenh mk 
     WHERE mk.medicalrecordid = p.medicalrecordid_kb 
     LIMIT 1) AS ly_do_den_kham,
    nv_phu1_dieu_tri.nhanvienname AS bacsi_phu1_dieu_tri,
    'http://116.97.118.146/dieutri?uid=' || encode_ehc_iv(p.patientid::text) AS linkkq
FROM
    tb_medicalrecord_henkham m
LEFT JOIN
    tb_patientrecord p ON m.patientrecordid = p.patientrecordid
LEFT JOIN
    tb_room r ON m.dm_roomid = r.roomid
LEFT JOIN
    tb_nhanvien nv_dieutri ON nv_dieutri.nhanvienid = (SELECT userid_dieutri FROM tb_medicalrecord WHERE medicalrecordid = m.medicalrecordid)
LEFT JOIN
    tb_nhanvien nv_phu1 ON nv_phu1.nhanvienid = (SELECT userid_phu1 FROM tb_medicalrecord WHERE medicalrecordid = m.medicalrecordid)
LEFT JOIN
    tb_nhanvien nv_dieuduong ON nv_dieuduong.nhanvienid = (SELECT userid_dieuduong FROM tb_medicalrecord WHERE medicalrecordid = m.medicalrecordid)
LEFT JOIN
    tb_nguoigioithieu nk ON nk.nguoigioithieuid = p.nguoigioithieuid
LEFT JOIN
    tb_medicalrecord md ON md.medicalrecordid = m.medicalrecordid
LEFT JOIN (
    SELECT 
        patientrecordid,
        MAX(userid_phu1) AS userid_phu1
    FROM tb_treatment
    GROUP BY patientrecordid
) t ON t.patientrecordid = p.patientrecordid
LEFT JOIN tb_nhanvien nv_phu1_dieu_tri ON nv_phu1_dieu_tri.nhanvienid = t.userid_phu1
),

-- CTE 2: Lấy danh sách bệnh nhân đã khám xong, CHƯA CÓ lịch hẹn tái khám
tb_temp_khong_hen AS (
  SELECT
    (SELECT roomname FROM tb_room WHERE roomid = p.roomid_kb LIMIT 1) AS phong_kham,
    TO_CHAR(p.patientrecorddate, 'YYYY-MM-DD') AS ngay_kham,
    p.patientcode AS ma_benh_nhan, 
    p.patientrecordid AS ma_ho_so,
    p.patientname AS ten_benh_nhan,
    p.patientphone AS so_dien_thoai,
    (SELECT nhanvienname 
     FROM tb_nhanvien 
     WHERE nhanvienid = (SELECT userid_phu1 
                         FROM tb_medicalrecord 
                         WHERE medicalrecordid = p.medicalrecordid_kb LIMIT 1) 
     LIMIT 1) AS bac_si_kham,
    p.chandoan_out_main_icd10 || '-' || p.chandoan_out_main AS ket_luan, 
    p.chandoan_out_ex_icd10 || '-' || p.chandoan_out_ex AS ket_qua,
    (SELECT nguoigioithieuname 
     FROM tb_nguoigioithieu 
     WHERE nguoigioithieuid = p.nguoigioithieuid LIMIT 1) AS nguon_khach,
    (SELECT dm_hoahongnguoigioithieu_nguoigioithieuname 
     FROM tb_dm_hoahongnguoigioithieu_nguoigioithieu 
     WHERE dm_hoahongnguoigioithieu_nguoigioithieuid = 
           (SELECT dm_hoahongnguoigioithieu_nguoigioithieuid 
            FROM tb_nguoigioithieu 
            WHERE nguoigioithieuid = p.nguoigioithieuid LIMIT 1)
     LIMIT 1) AS nhom_nguon_khach, 
    CASE 
        WHEN p.medicalrecorddate_out :: date = '0001-01-01' THEN NULL
        ELSE p.medicalrecorddate_out :: date
    END AS NgayRaVien,
    CASE 
        WHEN p.dm_gioitinhid = 1 THEN 'Nam' 
        WHEN p.dm_gioitinhid = 2 THEN 'Nữ' 
        ELSE '' 
    END AS gioi_tinh,
    p.birthdayyear AS nam_sinh,
    CONCAT_WS(', ',
             (SELECT dm_xaname FROM tb_dm_xa WHERE dm_xaid = p.dm_xacode LIMIT 1),
             (SELECT dm_huyenname FROM tb_dm_huyen WHERE dm_huyenid = p.dm_huyencode LIMIT 1),
             (SELECT dm_tinhname FROM tb_dm_tinh WHERE dm_tinhid = p.dm_tinhcode LIMIT 1)) AS dia_chi,
    p.patientphone AS sdt, -- Thêm cột này để khớp với CTE 1
    NULL AS ma_phan_nhom,
    NULL AS ten_phan_nhom,
    (SELECT loidanbacsi FROM tb_medicalrecord AS m WHERE m.medicalrecordid = p.medicalrecordid_kb LIMIT 1) AS noi_dung_hen_tai_kham,
    (SELECT nguoinhap FROM tb_medicalrecord_henkham AS m WHERE m.medicalrecordid = p.medicalrecordid_kb LIMIT 1) AS ma_bac_si_hen_tai_kham,
    (SELECT TO_CHAR(m.ngayhen, 'YYYY-MM-DD') 
     FROM tb_medicalrecord_henkham AS m 
     WHERE m.medicalrecordid = p.medicalrecordid_kb 
     LIMIT 1) AS ngay_hen_tai_kham,
    (SELECT nhanvienname 
     FROM tb_nhanvien 
     WHERE nhanvienid = (SELECT nguoinhap 
                         FROM tb_medicalrecord_henkham
                         WHERE medicalrecordid = p.medicalrecordid_kb LIMIT 1) 
     LIMIT 1) AS ten_bac_si_hen_tai_kham,
    NULL AS ngay_tao, -- Thêm cột này để khớp với CTE 1
    med.tomtatketquacls AS ket_qua_can_lam_sang,
    (SELECT STRING_AGG(DISTINCT CONCAT(md.medicinename, ': ', sd.thuoc_soluong, ' viên ', sd.thuoc_huongdansudung, ' (', sd.thuoc_duongdung, ')'), '; ')
     FROM tb_servicedata sd
     LEFT JOIN tb_medicinedata md ON sd.serviceid = md.medicineid
     WHERE sd.patientrecordid = p.patientrecordid 
     AND sd.serviceid_thuocmaster IS NOT NULL
     AND md.medicinename IS NOT NULL
     AND sd.thuoc_soluong IS NOT NULL
     AND sd.thuoc_soluong > 0
     AND sd.thuoc_huongdansudung IS NOT NULL
     AND sd.thuoc_duongdung IS NOT NULL
     LIMIT 1) AS don_thuoc,
    (SELECT lydodenkham 
     FROM tb_medicalrecord_khambenh mk 
     WHERE mk.medicalrecordid = p.medicalrecordid_kb 
     LIMIT 1) AS ly_do_den_kham,
    nv_phu1_dieu_tri.nhanvienname AS bacsi_phu1_dieu_tri,
    'http://116.97.118.146/dieutri?uid=' || encode_ehc_iv(p.patientid::text) AS linkkq
  FROM tb_patientrecord p
  LEFT JOIN tb_medicalrecord med ON med.medicalrecordid = p.medicalrecordid_kb
  LEFT JOIN (
      SELECT 
          patientrecordid,
          MAX(userid_phu1) AS userid_phu1
      FROM tb_treatment
      GROUP BY patientrecordid
  ) t ON t.patientrecordid = p.patientrecordid
  LEFT JOIN tb_nhanvien nv_phu1_dieu_tri ON nv_phu1_dieu_tri.nhanvienid = t.userid_phu1
  WHERE (med.dm_medicalrecordstatusid IS NULL OR med.dm_medicalrecordstatusid != 0)
    AND NOT EXISTS (
        SELECT 1
        FROM tb_medicalrecord_henkham m
        WHERE m.patientrecordid = p.patientrecordid
        AND m.ngaynhap >= 'date_start_scan_placeholder'
        AND m.ngaynhap <= 'date_end_scan_placeholder'
    )
)
-- Kết hợp cả hai nhóm
SELECT DISTINCT 
    ngay_kham, NgayRaVien, phong_kham, bac_si_kham, bacsi_phu1_dieu_tri,
    ma_ho_so, ma_benh_nhan, nguon_khach, nhom_nguon_khach, ten_benh_nhan, 
    gioi_tinh, nam_sinh, so_dien_thoai, dia_chi, ket_qua, ket_luan, ma_phan_nhom,
    ten_phan_nhom, ngay_hen_tai_kham, noi_dung_hen_tai_kham, 
    ma_bac_si_hen_tai_kham, ten_bac_si_hen_tai_kham, ket_qua_can_lam_sang, 
    don_thuoc, ly_do_den_kham, linkkq
FROM tb_temp_co_hen 
WHERE so_dien_thoai IS NOT NULL AND TRIM(so_dien_thoai) <> '' 
  AND nhom_nguon_khach NOT ILIKE '%Khám sức khỏe%' -- Lọc KSK
  AND ket_luan NOT ILIKE 'Z00.0-Khám sức khỏe tổng quát' -- Lọc KSK
  AND ngay_tao >= 'date_start_scan_placeholder' -- Lọc nhóm CÓ hẹn theo NGÀY TẠO HẸN
  AND ngay_tao <= 'date_end_scan_placeholder'
  
UNION ALL

SELECT DISTINCT 
    ngay_kham, NgayRaVien, phong_kham, bac_si_kham, bacsi_phu1_dieu_tri,
    ma_ho_so, ma_benh_nhan, nguon_khach, nhom_nguon_khach, ten_benh_nhan, 
    gioi_tinh, nam_sinh, so_dien_thoai, dia_chi, ket_qua, ket_luan, ma_phan_nhom,
    ten_phan_nhom, ngay_hen_tai_kham, noi_dung_hen_tai_kham, 
    ma_bac_si_hen_tai_kham, ten_bac_si_hen_tai_kham,ket_qua_can_lam_sang,
    don_thuoc,ly_do_den_kham, linkkq
FROM tb_temp_khong_hen 
WHERE so_dien_thoai IS NOT NULL 
  AND TRIM(so_dien_thoai) <> '' 
  AND nhom_nguon_khach NOT ILIKE '%Khám sức khỏe%' -- Lọc KSK
  AND ket_luan NOT ILIKE 'Z00.0-Khám sức khỏe tổng quát' -- Lọc KSK
  AND ngay_kham >= 'date_start_scan_placeholder' -- Lọc nhóm KHÔNG hẹn theo NGÀY KHÁM
  AND ngay_kham <= 'date_end_scan_placeholder'

ORDER BY ngay_kham, ma_ho_so;