package com.example.bhxh_api.dto;

import java.util.List;
//DTO chứa danh sách + thông tin phân trang
public record PageResponse<T>(
    List<T> content,            // Danh sách dữ liệu của trang hiện tại
    int pageNo,                 // Số trang hiện tại (bắt đầu từ 0)
    int pageSize,               // Số bản ghi mỗi trang
    long totalElements,         // Tổng số bản ghi
    int totalPages              // Tổng số trang
) {}