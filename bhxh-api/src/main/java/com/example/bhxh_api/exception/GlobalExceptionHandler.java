package com.example.bhxh_api.exception;

import com.example.bhxh_api.dto.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    // Xảy ra khi truyền sai kiểu, vd page=abc thay vì số nguyên
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiResponse<Object>> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        log.error("Invalid parameter type: {}", ex.getMessage(), ex);
        String message = String.format("Tham số '%s' không hợp lệ, giá trị nhận được: '%s'",
                ex.getName(), ex.getValue());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ApiResponse<>(400, message, null));
    }

    // Xảy ra khi thiếu tham số bắt buộc, vd không truyền keyword
    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiResponse<Object>> handleMissingParam(MissingServletRequestParameterException ex) {
        log.error("Missing required parameter: {}", ex.getMessage(), ex);
        String message = String.format("Thiếu tham số bắt buộc: '%s'", ex.getParameterName());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ApiResponse<>(400, message, null));
    }

    // Bắt tất cả lỗi khác chưa được xử lý riêng -> trả 500, không để lộ stacktrace ra client
    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Object>> handleGeneral(Exception ex) {
        log.error("Unexpected error occurred: {}", ex.getMessage(), ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(new ApiResponse<>(500, "Lỗi hệ thống, vui lòng thử lại sau", null));
    }
}
