package com.example.bhxh_api.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Entity

//Map với bảng qttg_header trong database
@Table(name = "qttg_header")

@Getter
@Setter
public class BhxhHeader {

    @Id
    private Long id;   // KHÔNG dùng @GeneratedValue - id được Spark ghi tường minh (= ID gốc từ Oracle)

    @Column(name = "full_name")
    private String fullName;

    @Column(name = "nld_id")
    private Long nldId;

    @Column(name = "so_so_bhxh", nullable = false, unique = true)
    private String soSoBhxh;

    @Column(name = "thang_bd")
    private String thangBd;

    @Column(name = "thang_kt")
    private String thangKt;

    //Quan hệ 1-N với BhxhDetail. LAZY = chỉ load khi cần.
    //mappedBy = "header" → Bên BhxhDetail mới là bên sở hữu quan hệ (có khóa ngoại).
    //FetchType.LAZY → Khi lấy BhxhHeader, danh sách details chưa được load. Chỉ load khi bạn gọi h.getDetails().
    //Vì vậy trong Service mới cần @Transactional(readOnly = true), nếu không sẽ bị lỗi LazyInitializationException.
    @OneToMany(mappedBy = "header", fetch = FetchType.LAZY)
    private List<BhxhDetail> details;
}