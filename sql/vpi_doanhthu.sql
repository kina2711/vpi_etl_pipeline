WITH
    tb_temp AS (
        SELECT
            medicalrecordid,
             CASE
                WHEN LENGTH(yeucaukham) > 0 THEN do_roomid
                ELSE roomid
            END AS roomid,
            do_roomid,
            CASE
                WHEN LENGTH(yeucaukham) > 0 THEN do_departmentid
                ELSE departmentid
            END AS departmentid,
            do_departmentid,
            do_userid,
            userid_phu1,
            ROW_NUMBER() OVER (PARTITION BY medicalrecordid ORDER BY treatmentdate, LENGTH(yeucaukham) DESC) AS rownumber
        FROM
            tb_treatment
    ),
    
    treatment AS (
        SELECT * 
        FROM tb_temp 
        WHERE rownumber = 1
    ),

    -- Lọc lấy ra hóa đơn thu phiếu dịch vụ hợp lệ
    tempInvoicedv AS (
        SELECT *
        FROM tb_invoice
        WHERE dm_invoice_typeid = 1  -- chỉ lấy loại phiếu thu tiền dịch vụ 3,4 là ứng, hoàn ứng
            AND huyphieu_status = 0  -- loại bỏ phiếu bị hủy
    ),
	
    servicefull AS (
        SELECT
            p.patientrecorddate :: date AS NgayVaoVien,
			sv.servicedatadate::date AS NgayThucHien,
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
                WHEN p.dm_patientrecordtypeid = 1 THEN 'Ngoại trú'
                ELSE 'Nội trú' 
            END AS PhanLoai,

            CASE
                WHEN sv.dm_servicegroupid IN (7,8) THEN 'THUOC'
                ELSE 'DVKT'
            END AS PhanNhom,

			CASE
                WHEN p.dm_patientobjectid = 1 THEN 'BH'
                ELSE 'DV'
            END AS PhanLoaiDoiTuongKH,
			
            CASE
                WHEN rc.patientrecordid IS NULL THEN 'DTB'
                ELSE 'KCB'
            END AS PhanloaiKCB,

            sv.patientrecordid AS MaHoSo,
            p.patientcode AS MaKhachHang,
            p.patientname AS TenKhachHang,
            p.birthdayyear AS NamSinh,

            CASE
                WHEN p.dm_gioitinhid = '2' THEN 'Nữ'
                ELSE 'Nam'
            END AS GioiTinh,

            p.patientphone AS SoDienThoai,
			concat_ws(', ', p.dm_xaname,p.dm_huyenname, p.dm_tinhname) AS DiaChi,

            COALESCE(H.dm_hoahongnguoigioithieu_nguoigioithieuname, N'TỰ ĐẾN') AS TenNhomNguonKhach,
            COALESCE(NK.nguoigioithieuname, N'Tự đến') AS TenNguonKhach,
			NK.nguoigioithieucode AS MaNguonKhach,

            -- Thông tin vào khám bệnh: khoa, phòng, bác sĩ
            D.departmentname AS KhoaChiDinh,
            R.roomname AS PhongChiDinh,
            D1.departmentname AS KhoaThucHien,
            R1.roomname AS PhongThucHien,
            tm.userid_phu1 AS MaBacSyChiDinh,
            NV2.nhanvienname AS TenBacSyChiDinh,
            sv.do_userid AS MaBacSyThucHien,
            NV1.nhanvienname AS TenBacSyThucHien,
            sv.servicecode AS MaChiTieu,
            sv.servicename AS TenChiTieu,

            CASE
                WHEN sv.dm_serviceobjectid = 3 THEN 'BH'
                ELSE 'DV'
            END AS PhanLoaiDoiTuongDichVu,

            COALESCE(SubNhom.dm_servicesubgroupcode, NDV.dm_servicegroupcode) AS MaNhomNho,
            COALESCE(SubNhom.dm_servicesubgroupname, NDV.dm_servicegroupname) AS TenNhomNho,
            NDV.dm_servicegroupcode AS MaNhom,

            sv.tongchiphi - sv.tongnguonkhac - COALESCE((sv.tongbenhnhan)/IV.sotienphieu * IV.sotienmiengiam, 0) AS DoanhThu,
            sv.tongnguonkhac AS GiamGia,
			sv.tongchiphi AS Total,
            sv.t_bhtt AS BHCT,
            sv.t_bncct AS BNCCT,
            sv.t_bntt - COALESCE((sv.tongbenhnhan)/IV.sotienphieu * IV.sotienmiengiam, 0) AS BNCT,
			
            CASE
			    WHEN p.danopdutienkhamdichvu = 1 THEN 'Đã nộp'
				ELSE 'Chưa nộp'
			END AS ThuTien,
            case
                WHEN sv.patientrecordid IN (select patientrecordid from tb_servicedata where servicecode  IN ('DV_0322','12.0319.1190','12.0320.1190','DV_0318','10.0549.0494','15.0151.2036','10.0555.0494','DV_0310','DV_0306','DV_0314',
                    '27.0273.0473','13.0115.0650','DV_0313','27.0187.2039','10.0411.0584.2','DV_0336') ) THEN 'Phẫu thuật'
                when sv.patientrecordid IN (SELECT patientrecordid FROM tb_servicedata WHERE dm_servicesubgroupid ='100049') and p.patientrecorddate::date >= '2025-07-01'  then 'Ngoại phẫu' 
                WHEN p.dm_patientrecordtypeid = 2 THEN 'Nội trú'
                WHEN sv.patientrecordid IN (select patientrecordid from tb_servicedata where do_roomid  IN (65,110,270,271,272)) THEN 'Tiêu hóa'
                                WHEN sv.patientrecordid IN (select patientrecordid from tb_servicedata where roomid  IN (65,110,270,271,272)) THEN 'Tiêu hóa'
                ELSE 'Đa Khoa'
            END AS DaKhoaOrTieuHoa,
			sv.lydomiengiam AS lydogiamgia

        FROM
            tb_servicedata AS sv
            LEFT JOIN tb_patientrecord AS p ON sv.patientrecordid = p.patientrecordid
            LEFT JOIN tb_reception AS rc ON sv.patientrecordid = rc.patientrecordid 
                AND DATE(sv.servicedatadate) = DATE(rc.receptiondate)
            LEFT JOIN treatment AS tm ON sv.medicalrecordid = tm.medicalrecordid
            LEFT JOIN tb_medicalrecord AS med ON sv.medicalrecordid = med.medicalrecordid
            LEFT JOIN tb_room AS R ON R.roomid = tm.roomid
            LEFT JOIN tb_room AS R1 ON R1.roomid = sv.do_roomid
            LEFT JOIN tb_department D ON D.departmentid = tm.departmentid
            LEFT JOIN tb_department D1 ON D1.departmentid = sv.do_departmentid
            LEFT JOIN tb_nhanvien AS NV2 ON NV2.nhanvienid = tm.userid_phu1
            LEFT JOIN tb_nhanvien AS NV1 ON NV1.nhanvienid = sv.do_userid
            LEFT JOIN tb_dm_servicegroup AS NDV ON NDV.dm_servicegroupid = sv.dm_servicegroupid
            LEFT JOIN tb_dm_servicesubgroup AS SubNhom ON SubNhom.dm_servicesubgroupid = sv.dm_servicesubgroupid
            LEFT JOIN tempInvoicedv AS IV ON IV.invoiceid = sv.invoiceid
            LEFT JOIN tb_nguoigioithieu AS NK ON NK.nguoigioithieuid = sv.nguoigioithieuid
            LEFT JOIN tb_patient AS pt ON pt.patientid = p.patientid
            LEFT JOIN tb_dm_hoahongnguoigioithieu_nguoigioithieu AS H ON H.dm_hoahongnguoigioithieu_nguoigioithieuid = NK.dm_hoahongnguoigioithieu_nguoigioithieuid
        WHERE
            sv.soluong != 0
			AND (med.dm_medicalrecordstatusid IS NULL OR med.dm_medicalrecordstatusid != 0)
            AND (med.dm_hinhthucravienid IS NULL OR med.dm_hinhthucravienid != 7)
            AND p.patientname NOT LIKE '%TEST%'
    ),

    treatment2 AS (
        SELECT
            t1.treatmentid AS treatmentid,
            t2.medicalrecordid AS medicalrecordid,
            t2.roomid AS roomid,
            t2.do_roomid AS do_roomid,
            t2.departmentid,
            t2.do_departmentid,
            t2.do_userid AS do_userid,
            t2.userid_phu1 AS userid_phu1
        FROM
            tb_treatment AS t1
            LEFT JOIN treatment AS t2 ON t1.medicalrecordid = t2.medicalrecordid
    ),

    tempmedicinedata AS (
        SELECT *
        FROM tb_medicinedata
        WHERE medicinebillid IN (
            SELECT medicinebillid
            FROM tb_medicinebill
            WHERE huyphieu_status = 0
                AND thungan_medicinebilldate <> '0001-01-01'
        )
    ),

    medicinefull AS (
        SELECT
            CASE 
			    WHEN bill.patientrecordid != 0 THEN p.patientrecorddate :: date 
            ELSE bill.medicinebilldate :: date END AS NgayVaoVien,
			bill.medicinebilldate::date AS NgayThucHien,
            CASE 
                WHEN bill.thungan_medicinebilldate :: date = '0001-01-01' 
				    AND bill.thungan_medicinebilldate != '0001-01-01' 
                    THEN bill.finish_medicinebilldate :: date
				WHEN bill.thungan_medicinebilldate = '0001-01-01' THEN null
                ELSE bill.thungan_medicinebilldate :: date
            END AS NgayRaVien,

            CASE 
                WHEN p.dm_patientrecordtypeid = 1 THEN 'Ngoại trú'
				WHEN bill.patientrecordid = 0 THEN 'Ngoại trú'
                ELSE 'Nội trú' 
            END AS PhanLoai,
            
            'THUOC' AS PhanNhom,
            
            CASE
                WHEN p.dm_patientobjectid = 1 THEN 'BH'
                ELSE 'DV'
            END AS PhanLoaiDoiTuongKH,
			
            CASE
                WHEN bill.patientrecordid = 0 THEN 'THUOCLE'
                WHEN rc.patientrecordid IS NULL THEN 'DTB'
                ELSE 'KCB'
            END AS PhanloaiKCB,
			
            CASE
                WHEN bill.patientrecordid = 0 THEN null
                ELSE bill.patientrecordid
            END AS MaHoSo,
            p.patientcode AS MaKhachHang,
            p.patientname AS TenKhachHang,
            p.birthdayyear AS NamSinh,

            CASE
                WHEN p.dm_gioitinhid = '2' THEN 'Nữ'
                ELSE 'Nam'
            END AS GioiTinh,

            p.patientphone AS SoDienThoai,
			concat_ws(', ', p.dm_xaname,p.dm_huyenname, p.dm_tinhname) AS DiaChi,

            COALESCE(H.dm_hoahongnguoigioithieu_nguoigioithieuname, N'Tự đến') AS TenNhomNguonKhach,
            COALESCE(NK.nguoigioithieuname, N'Tự đến') AS TenNguonKhach,
			NK.nguoigioithieucode AS MaNguonKhach,
      
            -- Thông tin vào khám bệnh: khoa, phòng, bác sĩ
            D.departmentname AS KhoaChiDinh,
            R.roomname AS PhongChiDinh,
            D1.departmentname AS KhoaThucHien,
            R1.roomname AS PhongThucHien,
            treatment2.userid_phu1 AS MaBacSyChiDinh,
            NV2.nhanvienname AS TenBacSyChiDinh,
            treatment2.do_userid AS MaBacSyThucHien,
            NV1.nhanvienname AS TenBacSyThucHien,

            DTCT.medicinecode AS MaChiTieu,
            DTCT.medicinename AS TenChiTieu,

            'DV' AS PhanLoaiDoiTuongDichVu,
            'THUOC' AS MaNhomNho,
            'THUOC' AS TenNhomNho,
            'THUOC' AS MaNhom,
            DTCT.medicine_gia * DTCT.soluong AS DoanhThu,
            0 AS GiamGia,
            DTCT.medicine_gia * DTCT.soluong AS Total,
            0 AS BHCT,
            0 AS BNCCT,
            DTCT.medicine_gia * DTCT.soluong AS BNCT,
			CASE
               WHEN p.duyetketoan_is = 1 THEN 'Đã nộp'
               ELSE 'Chưa nộp'
            END AS ThuTien,
            CASE
                WHEN p.dm_patientrecordtypeid = 2 THEN 'Nội trú'
                WHEN bill.patientrecordid IN (select patientrecordid from tb_servicedata where servicecode  IN ('DV_0322','12.0319.1190','12.0320.1190','DV_0318','10.0549.0494','15.0151.2036','10.0555.0494','DV_0310','DV_0306','DV_0314',
	            '27.0273.0473','13.0115.0650','DV_0313','27.0187.2039','10.0411.0584.2','DV_0336') ) THEN 'Phẫu thuật'
                when bill.patientrecordid  IN (SELECT patientrecordid FROM tb_servicedata WHERE dm_servicesubgroupid ='100049') and p.patientrecorddate::date >= '2025-07-01'  then 'Ngoại phẫu'
                WHEN bill.patientrecordid IN (SELECT patientrecordid FROM tb_servicedata WHERE do_roomid IN (65,110,270,271,272)) THEN 'Tiêu hóa'
                WHEN bill.patientrecordid IN (SELECT patientrecordid FROM tb_servicedata WHERE roomid IN (65,110,270,271,272)) THEN 'Tiêu hóa'
                ELSE 'Đa Khoa'
            END AS DaKhoaOrTieuHoa,
			null AS lydogiamgia
			
        FROM
            tempmedicinedata AS DTCT
            LEFT JOIN tb_medicinebill AS bill ON bill.medicinebillid = DTCT.medicinebillid
            LEFT JOIN tb_patientrecord AS p ON p.patientrecordid = bill.patientrecordid
            LEFT JOIN tb_reception AS rc ON bill.patientrecordid = rc.patientrecordid
                AND DATE(bill.thungan_medicinebilldate) = DATE(rc.receptiondate)
            LEFT JOIN treatment2 ON bill.treatmentid = treatment2.treatmentid
            LEFT JOIN tb_room AS R ON R.roomid = treatment2.roomid
            LEFT JOIN tb_room AS R1 ON R1.roomid = treatment2.do_roomid
            LEFT JOIN tb_department D ON D.departmentid = treatment2.departmentid
            LEFT JOIN tb_department D1 ON D1.departmentid = treatment2.do_departmentid
            LEFT JOIN tb_nhanvien AS NV2 ON NV2.nhanvienid = treatment2.userid_phu1
            LEFT JOIN tb_nhanvien AS NV1 ON NV1.nhanvienid = treatment2.do_userid
            LEFT JOIN tb_nguoigioithieu AS NK ON NK.nguoigioithieuid = p.nguoigioithieuid
            LEFT JOIN tb_patient AS pt ON pt.patientid = p.patientid
            
            LEFT JOIN tb_dm_hoahongnguoigioithieu_nguoigioithieu AS H ON H.dm_hoahongnguoigioithieu_nguoigioithieuid = NK.dm_hoahongnguoigioithieu_nguoigioithieuid
        WHERE
		bill.huyphieu_status = 0
	),

    Combine AS (
        SELECT * FROM servicefull
        UNION ALL
        SELECT * FROM medicinefull
	    ORDER BY MaHoSo, NgayThucHien
    )

SELECT ngayvaovien,	ngaythuchien, ngayravien, phanloai,	phannhom, phanloaidoituongkh, phanloaikcb, mahoso, makhachhang,	tenkhachhang,
namsinh, gioitinh, sodienthoai, diachi, tennhomnguonkhach, tennguonkhach, khoachidinh, phongchidinh, khoathuchien, phongthuchien,
mabacsychidinh,	tenbacsychidinh, mabacsythuchien, tenbacsythuchien, machitieu, tenchitieu, phanloaidoituongdichvu, manhomnho, tennhomnho,
manhom,	doanhthu, giamgia, total, bhct, bncct, bnct, thutien, dakhoaortieuhoa, lydogiamgia,n."TenSale"
FROM Combine 

LEFT JOIN (
    SELECT 
       "MaKH", "Tennguonkhach", "TenSale", "StartDate", "Nhomnguonkhach",
        CASE 
            WHEN "EndDate" IS NULL THEN DATE 'date_end_scan_placeholder'
            ELSE "EndDate"::date
        END AS date_end
    FROM bvvp_stagging.ds_nguon_khach
) AS n
  ON Combine.MaNguonKhach = n."MaKH"
  AND Combine.tennhomnguonkhach = n."Nhomnguonkhach"
  AND Combine.ngayvaovien BETWEEN n."StartDate" AND n.date_end

WHERE ngayvaovien >= 'date_start_scan_placeholder'
  AND ngayvaovien <= 'date_end_scan_placeholder'