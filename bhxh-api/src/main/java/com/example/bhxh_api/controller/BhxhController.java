package com.example.bhxh_api.controller;

//Các DTO (Data Transfer Object) dùng để đóng gói dữ liệu trả về client.
import com.example.bhxh_api.dto.ApiResponse;
import com.example.bhxh_api.dto.BhxhResponse;
import com.example.bhxh_api.dto.PageResponse;

//Tầng xử lý logic nghiệp vụ mà Controller sẽ gọi
import com.example.bhxh_api.service.BhxhService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.NotBlank;
//annotation của thư viện Lombok, tự sinh constructor
import lombok.RequiredArgsConstructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
//Tất cả API trong class này đều bắt đầu bằng /api/v1/bhxh
@RequestMapping("/api/v1/bhxh")
@RequiredArgsConstructor
// @Validated                    // ← quan trọng
@Tag(name = "BHXH Search API", description = "Tra cứu quá trình tham gia BHXH")
public class BhxhController {

    //ghi log phục vụ theo dõi, debug khi hệ thống chạy thật.
    private static final Logger log = LoggerFactory.getLogger(BhxhController.class);

    //Khai báo service
    private final BhxhService service;

    @GetMapping("/search")
    @Operation(summary = "Tìm kiếm theo mã BHXH, mã người lao động (NLD_ID) hoặc tên người lao động")
    //Khai báo method search, kiểu trả về là ApiResponse<PageResponse<BhxhResponse>> — 1 kiểu generic lồng nhau
    public ApiResponse<PageResponse<BhxhResponse>> search(

            //Mô tả tham số
            @Parameter(description = "Từ khóa - mã BHXH (so_so_bhxh) hoặc mã NLD (nld_id)", required = false)
            @RequestParam(required = false) String keyword,

            @Parameter(description = "Số trang, bắt đầu từ 0")
            @RequestParam(defaultValue = "0") Integer page,

            @Parameter(description = "Số bản ghi mỗi trang")
            @RequestParam(defaultValue = "100") Integer size) {
        
        //Ghi log để dễ theo dõi khi chạy thật.
        log.info("GET /api/v1/bhxh/search - keyword={}, page={}, size={}", keyword, page, size);

        //Gọi xuống Service để xử lý logic + lấy dữ liệu.
        PageResponse<BhxhResponse> data = service.search(keyword, page, size);
        
        //Đóng gói kết quả theo format
        return new ApiResponse<>(200, "Thành công", data);
    }
}