package com.example.bhxh_api.service;

import com.example.bhxh_api.dto.BhxhDetailResponse;
import com.example.bhxh_api.dto.BhxhResponse;
import com.example.bhxh_api.dto.PageResponse;
import com.example.bhxh_api.entity.BhxhDetail;
import com.example.bhxh_api.entity.BhxhHeader;
import com.example.bhxh_api.repository.BhxhHeaderRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class BhxhService {

    private final BhxhHeaderRepository repository;

    @Transactional(readOnly = true)
    public PageResponse<BhxhResponse> search(String keyword, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);

        // Đoán ý định: keyword toàn số -> tìm theo Key/ID (chính xác, B-tree index).
        // Có chữ -> tìm theo tên (fuzzy, trgm index).
        boolean isNumericKey = keyword.matches("\\d+");

        //lấy danh sách ID khớp điều kiện (đã phân trang ở tầng DB)
        Page<Long> idPage;
        if (isNumericKey) {
            Long nldId;
            try {
                nldId = Long.parseLong(keyword);
            } catch (NumberFormatException e) {
                nldId = -1L; // số quá lớn vượt Long -> chắc chắn không khớp, tránh crash
            }
            idPage = repository.findIdsByExactKey(keyword, nldId, pageable);
        } else {
            idPage = repository.findIdsByNameFuzzy(keyword, pageable);
        }

        List<Long> ids = idPage.getContent();

        // nếu không có ID nào thì không cần query thêm
        List<BhxhHeader> headers;
        if (ids.isEmpty()) {
            headers = new ArrayList<>();
        } else {
            headers = repository.findAllWithDetailsByIdIn(ids);
        }

        //Đoạn này dùng Map để so sánh tối ưu hơn khi lấy đúng thứ tự id đã phân trang để kh bị đảo thứ tự
        // đưa headers vào Map để tra cứu theo ID nhanh (O(1)),
        // thay vì phải duyệt tìm trong list mỗi lần (O(n))
        Map<Long, BhxhHeader> headerById = new HashMap<>();
        for (BhxhHeader h : headers) {
            headerById.put(h.getId(), h);
        }

        // duyệt đúng theo thứ tự "ids" (thứ tự đã phân trang ở bước 1)
        // để đảm bảo kết quả trả về không bị đảo thứ tự
        List<BhxhResponse> content = new ArrayList<>();
        for (Long id : ids) {
            BhxhHeader header = headerById.get(id);
            if (header != null) { // phòng ID không tìm thấy header tương ứng
                content.add(toDto(header));
            }
        }

        return new PageResponse<>(
                content,
                idPage.getNumber(),
                idPage.getSize(),
                idPage.getTotalElements(),
                idPage.getTotalPages()
        );
    }

    private BhxhResponse toDto(BhxhHeader h) {
        List<BhxhDetailResponse> details = new ArrayList<>();
        for (BhxhDetail d : h.getDetails()) {
            details.add(new BhxhDetailResponse(
                    d.getMaDonVi(),
                    d.getTenDonVi(),
                    d.getTuThang(),
                    d.getDenThang(),
                    d.getChucDanh(),
                    d.getNoiLamViec(),
                    d.getMucLuong()
            ));
        }

        return new BhxhResponse(h.getNldId(), h.getSoSoBhxh(), h.getFullName(), details);
    }
}