package com.example.bhxh_api.dto;

//DTO bọc ngoài cùng (code + message + data)
public record ApiResponse<T>(
    int status,
    String message,
    T data
) {}