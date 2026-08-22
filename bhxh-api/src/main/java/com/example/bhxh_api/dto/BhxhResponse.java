package com.example.bhxh_api.dto;

import java.util.List;

//DTO chứa thông tin sổ BHXH + danh sách detail
public record BhxhResponse(
    Long nldId,
    String soSoBhxh,
    String fullName,
    // String thangBd,
    // String thangKt,
    List<BhxhDetailResponse> details
) {}