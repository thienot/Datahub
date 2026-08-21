package com.example.bhxh_api.dto;

import java.util.List;

//DTO chứa thông tin sổ BHXH + danh sách detail
public record BhxhResponse(
    Long nldId,
    String fullName,
    String soSoBhxh,
    // String thangBd,
    // String thangKt,
    List<BhxhDetailResponse> details
) {}