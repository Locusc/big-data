package cn.locusc.bd.visualization.repository;

import cn.locusc.bd.visualization.domain.PeriodCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author Jay
 * 时段数据接口
 * 2022/11/28
 */
@Repository
public interface PeriodCountRepository extends JpaRepository<PeriodCount, String> {

    @Query(value = "select logtime,count from periodcount order by count desc limit 10", nativeQuery = true)
    List<PeriodCount> periodRank();

}
