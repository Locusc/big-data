package cn.locusc.bd.visualization.domain;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "newscount")
public class NewsCount {

    @Id
    @Column(name = "name")
    private String name;

    @Column(name = "count")
    private String count;

}
