package cn.locusc.bd.visualization.domain;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Data
@Entity
@Table(name = "periodcount")
public class PeriodCount {

    @Id
    @Column(name = "logtime")
    private String logTime;

    @Column(name = "count")
    private String count;

}
