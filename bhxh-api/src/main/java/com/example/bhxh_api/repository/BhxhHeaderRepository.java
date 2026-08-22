package com.example.bhxh_api.repository;

import com.example.bhxh_api.entity.BhxhHeader;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface BhxhHeaderRepository extends JpaRepository<BhxhHeader, Long> {

    @Query("SELECT h.id FROM BhxhHeader h")
    Page<Long> findAllIds(Pageable pageable);

    //Tìm theo KEY (so_so_bhxh hoặc nld_id): so khớp CHÍNH XÁC
    // (dấu =, không phải LIKE), xử lý tra cứu theo mã/ID 
    // tận dụng B-tree index (idx_qttg_header_so_bhxh, idx_qttg_header_nld_id),
    // tốc độ O(log n) dù bảng có hàng triệu dòng,
    // không cần quét gần hết bảng như LIKE '%...%'.
    @Query("SELECT h.id FROM BhxhHeader h WHERE h.soSoBhxh = :key OR h.nldId = :nldId")
    Page<Long> findIdsByExactKey(@Param("key") String key, @Param("nldId") Long nldId, Pageable pageable);

    //Tìm theo name: fuzzy match, chỉ dùng khi input không phải số thuần. 
    // Dựa vào trgm index (idx_qttg_header_full_name_trgm).
    @Query(
        value = "SELECT h.id FROM BhxhHeader h WHERE h.fullName LIKE CONCAT('%', :keyword, '%')",
        countQuery = "SELECT COUNT(h.id) FROM BhxhHeader h WHERE LOWER(h.fullName) LIKE LOWER(CONCAT('%', :keyword, '%'))"
    )
    Page<Long> findIdsByNameFuzzy(@Param("keyword") String keyword, Pageable pageable);

    // Fetch đầy đủ Header + Detail cho đúng tập id -> tránh N+1
    @Query("SELECT DISTINCT h FROM BhxhHeader h LEFT JOIN FETCH h.details WHERE h.id IN :ids")
    List<BhxhHeader> findAllWithDetailsByIdIn(@Param("ids") List<Long> ids);
}
