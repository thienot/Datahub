package com.example.bhxh_api.dto;

import java.math.BigDecimal;

//DTO chứa 1 quá trình đóng BHXH
public record BhxhDetailResponse(
    String maDonVi,
    String tenDonVi,
    String tuThang,
    String denThang,
    String chucDanh,
    String noiLamViec,
    BigDecimal mucLuong
) {}