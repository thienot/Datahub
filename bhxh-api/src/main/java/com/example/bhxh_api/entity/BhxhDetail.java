package com.example.bhxh_api.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;

@Entity
@Table(name = "qttg_detail")
@Getter
@Setter
public class BhxhDetail {

    @Id
    //ID không tự tăng. ID được Spark ghi tường minh từ Oracle sang.
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    //Nhiều Detail thuộc về 1 Header
    @ManyToOne(fetch = FetchType.LAZY)
    //Khóa ngoại trỏ về bảng qttg_header
    @JoinColumn(name = "header_id", nullable = false)
    private BhxhHeader header;

    @Column(name = "ma_don_vi")
    private String maDonVi;

    @Column(name = "ten_don_vi")
    private String tenDonVi;

    @Column(name = "tu_thang")
    private String tuThang;

    @Column(name = "den_thang")
    private String denThang;

    @Column(name = "chuc_danh")
    private String chucDanh;

    @Column(name = "noi_lam_viec")
    private String noiLamViec;

    @Column(name = "muc_luong")
    private BigDecimal mucLuong;
}